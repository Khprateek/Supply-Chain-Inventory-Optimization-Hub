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

# Seed for reproducible realistic data distributions
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


def worker_produce(worker_id, target_rate_per_worker):
    """Worker function to produce messages at a throttled, steady rate."""
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'queue.buffering.max.messages': 50_000,
        'linger.ms': 5,
        'batch.num.messages': 100,
        'compression.type': 'lz4'
    })

    events_produced = 0
    start_time = time.time()
    batch_size = max(1, min(20, target_rate_per_worker // 5)) if target_rate_per_worker > 0 else 50
    sleep_interval = (batch_size / target_rate_per_worker) if target_rate_per_worker > 0 else 0

    print(f"[Worker {worker_id}] Started throttled generator (Target: ~{target_rate_per_worker} events/sec)...", flush=True)

    running = True

    def _handle_shutdown(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    last_batch_time = time.time()

    while running:
        for _ in range(batch_size):
            if not running:
                break

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
                    producer.poll(0.2)

            events_produced += 1

        # Poll callbacks
        producer.poll(0)

        # Rate-limiting throttle
        if target_rate_per_worker > 0:
            elapsed = time.time() - last_batch_time
            sleep_needed = sleep_interval - elapsed
            if sleep_needed > 0:
                time.sleep(sleep_needed)
            last_batch_time = time.time()

        if events_produced % 500 == 0:
            elapsed_total = time.time() - start_time
            rate = events_produced / max(elapsed_total, 0.001)
            print(f"[Worker {worker_id}] Produced {events_produced:,} events | Current Rate: ~{rate:,.0f} events/sec", flush=True)

    print(f"[Worker {worker_id}] Flushing remaining queued messages...", flush=True)
    producer.flush(timeout=5)


def main():
    parser = argparse.ArgumentParser(description="Throttled Kafka Telemetry Producer")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker processes (default: 1)")
    parser.add_argument("--rate", type=int, default=100, help="Target total events per second (default: 100)")
    args = parser.parse_args()

    num_workers = max(1, args.workers)
    target_rate = max(0, args.rate)
    rate_per_worker = target_rate // num_workers if target_rate > 0 else 0

    print("==========================================================")
    print("      Supply Chain Telemetry Event Generator")
    print(f"  Target Rate: ~{target_rate} events/sec total")
    print(f"  Workers:     {num_workers} (each ~{rate_per_worker} events/sec)")
    print(f"  Broker:      {KAFKA_BROKER}")
    print("==========================================================")

    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_produce, args=(i, rate_per_worker))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nStopping telemetry generator gracefully...")
        for p in processes:
            if p.is_alive():
                p.terminate()
        for p in processes:
            p.join(timeout=5)


if __name__ == '__main__':
    main()
