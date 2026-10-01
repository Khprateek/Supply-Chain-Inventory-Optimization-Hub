import time
import json
import uuid
import random
from confluent_kafka import Producer
import multiprocessing

# Kafka configuration
KAFKA_BROKER = 'localhost:29092'

# Pre-generate templates to bypass Faker CPU bottleneck
PRODUCTS = [f"PRD-{random.randint(1000, 9999)}" for _ in range(100)]
WAREHOUSES = [f"WH-{random.randint(1, 10)}" for _ in range(10)]
CUSTOMERS = [f"CUST-{random.randint(100, 999)}" for _ in range(50)]

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        pass

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
    
    while True:
        event_type_choice = random.choice(["inventory", "sales"])
        
        if event_type_choice == "inventory":
            topic = "inventory_events"
            event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": time.time(),
                "product_id": random.choice(PRODUCTS),
                "warehouse_id": random.choice(WAREHOUSES),
                "quantity_change": random.randint(-50, 100),
                "event_type": random.choice(["RECEIPT", "PICK", "ADJUSTMENT"])
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
            
        # Async produce
        producer.produce(
            topic,
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        
        events_produced += 1
        
        # Poll periodically and print status
        if events_produced % 500 == 0:
            producer.poll(0)
            
        if events_produced % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"[Worker {worker_id}] Generated {events_produced:,} combined events so far... (Avg: {events_produced/elapsed:,.0f} msg/sec)")
            
        # SLOW DOWN: Sleep for a tiny fraction of a second to prevent CPU overload
        time.sleep(0.005)

def main():
    print(f"🚀 Starting Steady, Continuous Kafka Generator...")
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
        for p in processes:
            p.terminate()

if __name__ == '__main__':
    main()
