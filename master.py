from confluent_kafka import Producer, Consumer, KafkaException
import json
import pandas as pd
from itertools import chain
import pysubgroup as ps
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**producer_conf)

def distribute_selectors(lv1selectors):
    worker_ids = ['worker1', 'worker2', 'worker3']
    for i, lv1selector in enumerate(lv1selectors):
        worker_id = worker_ids[i % len(worker_ids)]  
        producer.produce(
            'work-topic-' + worker_id, 
            key=worker_id, 
            value=json.dumps(lv1selector)
        )
        producer.flush()

def aggregate_results(num_workers):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9091',
        'group.id': 'master-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['results-topic'])

    completed_workers = 0
    results = []

    while completed_workers < num_workers:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                continue

        data = json.loads(msg.value().decode('utf-8'))
        if data.get('status') == 'completed':
            completed_workers += 1
            # pass
        else:
            results.extend(data['discoveries'])

    consumer.close()
    return results

def topk_sg(num_workers):
    results = aggregate_results(num_workers)
    # top_k = sorted(results, key=lambda x: x['quality'], reverse=True)[:10]
    # results are in form of descriptions, need to be parsed for sorting
    top_k = sorted(results)[:10]
    return top_k


def main():
    data = pd.read_csv("C:\Projects\SoftwareLab_PreThesis\gene_test.csv")
    # target = ps.BinaryTarget("survival_category", "long")
    search_space = ps.create_selectors(data, ignore=["survival_category", "overall survival follow-up time"])
    ref_operator = ps.StaticSpecializationOperator(search_space)
    level1_selectors = chain.from_iterable(getattr(ref_operator, search_space))
    distribute_selectors(level1_selectors)
if __name__ == "__main__":
    main()