import time
import json
import uuid
import random
from confluent_kafka import Producer
import multiprocessing

# Kafka configuration
KAFKA_BROKER = 'localhost:29092'

import os

# Pre-generate templates to bypass Faker CPU bottleneck
GENERATOR_SEED = int(os.environ.get("GENERATOR_SEED", "42"))
random.seed(GENERATOR_SEED)

PRODUCTS = [f"PRD-{random.randint(1000, 9999)}" for _ in range(100)]
WAREHOUSES = sorted({f"WH-{random.randint(1, 10)}" for _ in range(10)})
CUSTOMERS = [f"CUST-{random.randint(100, 999)}" for _ in range(50)]

import threading
_error_count = 0
_error_lock = threading.Lock()

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    global _error_count
    if err is not None:
        with _error_lock:
            _error_count += 1
            ec = _error_count
        # Log every error; suppress repetitive logging at high error rates
        if ec <= 5 or ec % 100 == 0:
            print(f"[DELIVERY ERROR #{ec}] topic={msg.topic()} "
                  f"partition={msg.partition()} err={err}", flush=True)

def worker_produce(worker_id):
    """ Worker function to produce messages continuously at a steady pace """
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'queue.buffering.max.messages': 100_000,
        'linger.ms': 5,
        'batch.num.messages': 1000,
        'compression.type': 'lz4'
    })
    
    events_produced = 0
    start_time = time.time()
    
    print(f"[Worker {worker_id}] Started continuous generation for Sales & Inventory...")
    
    import signal
    
    running = True
    def _handle_shutdown(sig, frame):
        nonlocal running
        running = False
        
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)
    
    while running:
        event_type_choice = random.choice(["inventory", "sales"])
        
        if event_type_choice == "inventory":
            topic = "inventory_events"
            event_type = random.choice(["RECEIPT", "PICK", "ADJUSTMENT"])
            
            if event_type == "RECEIPT":
                quantity_change = random.randint(1, 100)    # always positive
            elif event_type == "PICK":
                quantity_change = random.randint(-50, -1)   # always negative
            else:  # ADJUSTMENT
                quantity_change = random.randint(-20, 20)   # signed, realistic
                
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
        while True:
            try:
                producer.produce(
                    topic,
                    key=event["product_id"].encode('utf-8'),
                    value=json.dumps(event).encode('utf-8'),
                    callback=delivery_report
                )
                break
            except BufferError:
                # Queue full — Kafka is not keeping up. Back off and drain.
                print(f"[Worker {worker_id}] Producer queue full. Backing off...", flush=True)
                producer.poll(1.0)   # block for 1s to drain callbacks and allow delivery
        
        events_produced += 1
        
        # Poll periodically and drain the callback queue
        if events_produced % 1000 == 0:
            # Yield CPU for 1ms per batch to cap max CPU utilization 
            # while letting librdkafka's internal queues and batching work normally.
            producer.poll(0.001)
            
        if events_produced % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"[Worker {worker_id}] Generated {events_produced:,} combined events so far... (Avg: {events_produced/elapsed:,.0f} msg/sec)")

    # Graceful drain on exit
    print(f"[Worker {worker_id}] Flushing {len(producer)} queued messages...", flush=True)
    remaining = producer.flush(timeout=15)   # wait up to 15s for delivery
    if remaining > 0:
        print(f"[Worker {worker_id}] WARNING: {remaining} messages not delivered at shutdown.", flush=True)

def main():
    print(f"Starting Steady, Continuous Kafka Generator...")
    print(f"Target: Continuous stream to topics 'inventory_events' and 'sales_events'")
    
    # Restrict to only 2 CPU cores to prevent laptop freezing
    num_workers = 2 
    
    print(f"Spawning exactly {num_workers} processes for a light, steady stream...")
    
    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_produce, args=(i,))
        p.start()
        processes.append(p)
        
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nStopping continuous generation...")
        import os, signal
        for p in processes:
            if os.name != 'nt':
                os.kill(p.pid, signal.SIGTERM)
        
        for p in processes:
            p.join(timeout=12)
            if p.is_alive():
                p.terminate()

if __name__ == '__main__':
    main()
