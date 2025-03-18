from confluent_kafka import Producer, Consumer, KafkaException
import json
import pandas as pd
import pysubgroup as ps
from itertools import chain
import logging

# Configure logging
# logging.basicConfig(filename='master_m3.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

producer_conf = {'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093'}
producer = Producer(**producer_conf)

class LoadBalancer:
    def __init__(self):
        self.worker_loads = {}

    def distribute_tasks(self, tasks, num_workers):
        # logging.debug("Distributing tasks among %s workers", num_workers)
        for i, task in enumerate(tasks):
            worker_id = i % num_workers
            self.assign_task(worker_id, task)

    def assign_task(self, worker_id, task):
        # logging.debug("Assigning task %s to worker %s", task, worker_id)
        if worker_id not in self.worker_loads:
            self.worker_loads[worker_id] = []
        self.worker_loads[worker_id].append(task)
        producer.produce('work-topic', key=str(worker_id), value=json.dumps(task))
        producer.flush()

    def get_heaviest_worker(self):
        return max(self.worker_loads, key=lambda k: len(self.worker_loads[k]))

    def update_load(self, worker_id, task_count):
        # logging.debug("Updating load for worker %s to %s", worker_id, task_count)
        self.worker_loads[worker_id] = task_count

class GlobalThresholdMaintainer:
    def __init__(self):
        self.global_threshold = float('-inf')

    def update_threshold(self, new_threshold):
        # logging.debug("Updating global threshold to %s", new_threshold)
        if new_threshold > self.global_threshold:
            self.global_threshold = new_threshold
            self.notify_workers()

    def notify_workers(self):
        # logging.debug("Notifying workers of new global threshold %s", self.global_threshold)
        producer.produce('threshold-topic', value=json.dumps({'threshold': self.global_threshold}))
        producer.flush()

class TerminationHandler:
    def __init__(self):
        self.top_k_subgroups = []

    def report_results(self, results):
        # logging.debug("Reporting results: %s", results)
        self.top_k_subgroups.extend(results)
        self.top_k_subgroups = sorted(self.top_k_subgroups, key=lambda x: x['quality'], reverse=True)[:10]

    def finalize(self):
        # logging.debug("Finalizing results")
        print("Top Discoveries:", self.top_k_subgroups)

load_balancer = LoadBalancer()
global_threshold_maintainer = GlobalThresholdMaintainer()
termination_handler = TerminationHandler()

def distribute_selectors(lv1selectors):
    num_partitions = 3
    for i, selector in enumerate(lv1selectors):
        try:
            # Convert selector to a JSON-serializable format
            serializable_selector = (
                selector.to_dict() if hasattr(selector, "to_dict") else str(selector)
            )
            load_balancer.assign_task(i % num_partitions, serializable_selector)
        except Exception as e:
            print(f"Error serializing selector {selector}: {e}")

def aggregate_results(num_workers):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093',
        'group.id': 'master-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['results-topic'])

    completed_workers = 0
    results = []

    while completed_workers < num_workers:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            if data.get('status') == 'completed':
                completed_workers += 1
            else:
                # Deserialize results if necessary
                results.extend(data)
        except Exception as e:
            print(f"Error processing results: {e}")

    consumer.close()
    return results

def main():
    try:
        data = pd.read_csv(r"data/gene_test.csv")
        search_space = ps.create_selectors(data, ignore=["survival_category", "overall survival follow-up time"])
        level1_selectors = list(chain.from_iterable(ps.StaticSpecializationOperator(search_space).search_space))

        distribute_selectors(level1_selectors)
        results = aggregate_results(num_workers=3)
        termination_handler.report_results(results)
        termination_handler.finalize()
    except KeyboardInterrupt:
        # logging.info("Master node interrupted by user")
        pass
    except Exception as e:
        # logging.error("Master node encountered an error: %s", e)
        print(f"Error: {e}")

if __name__ == "__main__":
    main()