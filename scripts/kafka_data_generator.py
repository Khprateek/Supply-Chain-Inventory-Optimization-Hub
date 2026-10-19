import time
import json
import uuid
import random
import argparse
import multiprocessing
import os
import signal
import threading
from confluent_kafka import Producer

# Kafka configuration
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')

# Pre-generate templates to bypass Faker CPU bottleneck during high-throughput stress tests
GENERATOR_SEED = int(os.environ.get("GENERATOR_SEED", "42"))
random.seed(GENERATOR_SEED)

PRODUCTS = [f"PRD-{random.randint(1000, 9999)}" for _ in range(100)]
WAREHOUSES = sorted({f"WH-{random.randint(1, 10)}" for _ in range(10)})
CUSTOMERS = [f"CUST-{random.randint(100, 999)}" for _ in range(50)]

_error_count = 0
_error_lock = threading.Lock()


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    global _error_count
    if err is not None:
        with _error_lock:
            _error_count += 1
            ec = _error_count
        if ec <= 5 or ec % 100 == 0:
            print(f"[DELIVERY ERROR #{ec}] topic={msg.topic()} partition={msg.partition()} err={err}", flush=True)


def worker_produce(worker_id, target_rate_per_worker=0, max_events=0):
    """
    Worker function to produce messages continuously.
    - Default (target_rate_per_worker=0): High-throughput mode (~15,000+ events/sec per worker)
      using librdkafka batching (100K queue buffer, 1000 batch size, 5ms linger, LZ4 compression).
    - Throttled (target_rate_per_worker>0): Controlled rate for low-resource testing.
    """
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'queue.buffering.max.messages': 100_000,
        'linger.ms': 5,
        'batch.num.messages': 1000,
        'compression.type': 'lz4'
    })

    events_produced = 0
    start_time = time.time()

    mode_str = f"Throttled (~{target_rate_per_worker} ev/s)" if target_rate_per_worker > 0 else "High-Throughput Unthrottled"
    print(f"[Worker {worker_id}] Started continuous generation ({mode_str})...", flush=True)

    running = True

    def _handle_shutdown(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    last_batch_time = time.time()

    while running:
        event_type_choice = random.choice(["inventory", "sales"])

        if event_type_choice == "inventory":
            topic = "inventory_events"
            event_type = random.choice(["RECEIPT", "PICK", "ADJUSTMENT"])
            if event_type == "RECEIPT":
                quantity_change = random.randint(1, 100)
            elif event_type == "PICK":
                quantity_change = random.randint(-50, -1)
            else:
                quantity_change = random.randint(-20, 20)

            event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": time.time(),
                "product_id": random.choice(PRODUCTS),
                "warehouse_id": random.choice(WAREHOUSES),
                "quantity_change": quantity_change,
                "event_type": event_type,
            }
        else:
            topic = "sales_events"
            event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": time.time(),
                "product_id": random.choice(PRODUCTS),
                "customer_id": random.choice(CUSTOMERS),
                "revenue": round(random.uniform(10.0, 500.0), 2),
                "units_sold": random.randint(1, 5)
            }

        # Async produce with natural backpressure
        while running:
            try:
                producer.produce(
                    topic,
                    key=event["product_id"].encode('utf-8'),
                    value=json.dumps(event).encode('utf-8'),
                    callback=delivery_report
                )
                break
            except BufferError:
                print(f"[Worker {worker_id}] Producer queue full (100K). Backing off...", flush=True)
                producer.poll(1.0)

        events_produced += 1

        if max_events > 0 and events_produced >= max_events:
            break

        # Periodic callback drain & pacing
        if events_produced % 1000 == 0:
            producer.poll(0)
            if target_rate_per_worker > 0:
                expected_elapsed = 1000.0 / target_rate_per_worker
                actual_elapsed = time.time() - last_batch_time
                if expected_elapsed > actual_elapsed:
                    time.sleep(expected_elapsed - actual_elapsed)
                last_batch_time = time.time()
            else:
                # Normal high-throughput pacing: 50ms yield per 1000 events (~15K-20K ev/s per worker)
                time.sleep(0.05)

        if events_produced % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"[Worker {worker_id}] Generated {events_produced:,} events... (Avg: {events_produced / max(elapsed, 0.001):,.0f} msg/sec)", flush=True)

    # Graceful drain on exit
    print(f"[Worker {worker_id}] Flushing {len(producer)} queued messages...", flush=True)
    remaining = producer.flush(timeout=15)
    if remaining > 0:
        print(f"[Worker {worker_id}] WARNING: {remaining} messages not delivered at shutdown.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="High-Throughput Kafka Telemetry Producer")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker processes (default: 2 for ~30K ev/s)")
    parser.add_argument("--rate", type=int, default=0, help="Optional target rate limit in events/sec (0 = normal high-throughput)")
    parser.add_argument("--max-events", type=int, default=0, help="Optional max events per worker before exiting (0 = infinite)")
    args = parser.parse_args()

    num_workers = max(1, args.workers)
    rate_per_worker = (args.rate // num_workers) if args.rate > 0 else 0

    print(f"Starting High-Throughput Continuous Kafka Generator...")
    print(f"Target: Continuous stream to topics 'inventory_events' and 'sales_events'")
    print(f"Spawning {num_workers} worker processes (Buffer: 100K msgs, Batch: 1000, Linger: 5ms, Compression: LZ4)...")

    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_produce, args=(i, rate_per_worker, args.max_events))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nStopping continuous generation...")
        for p in processes:
            if os.name != 'nt':
                os.kill(p.pid, signal.SIGTERM)
        for p in processes:
            p.join(timeout=12)
            if p.is_alive():
                p.terminate()


if __name__ == '__main__':
    main()
