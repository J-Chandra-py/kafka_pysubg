from confluent_kafka import Producer, Consumer, KafkaException
import json
import pandas as pd
import pysubgroup as ps
from itertools import chain
from kazoo.client import KazooClient
import sys
import logging

# Configure logging
# logging.basicConfig(filename='worker_m3.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

class ParallelBFS:
    def __init__(self, task, lv1selector=None):
        self.task = task
        self.lv1selector = lv1selector

    def execute(self):
        # logging.debug("Executing ParallelBFS with task: %s and lv1selector: %s", self.task, self.lv1selector)
        result = []
        queue = [(float("-inf"), ps.Conjunction([]))]
        operator = ps.StaticSpecializationOperator(self.task.search_space)
        self.task.qf.calculate_constant_statistics(self.task.data, self.task.target)

        while queue:
            _, sg = queue.pop(0)
            sGs = operator.refine(sg, self.task.search_space)
            for candidate_description in sGs:
                ps.add_if_required(result, sg, self.task.qf.evaluate(sg, self.task.target, self.task.data), self.task)
                if len(candidate_description) < self.task.depth:
                    queue.append((self.task.qf.optimistic_estimate(sg, self.task.target, self.task.data), sg))

        return ps.prepare_subgroup_discovery_result(result, self.task)

def report_results(producer, worker_id, discoveries):
    try:
        # Convert discoveries to a JSON-serializable format
        serializable_discoveries = [
            discovery.to_dict() if hasattr(discovery, "to_dict") else str(discovery)
            for discovery in discoveries
        ]
        producer.produce('results-topic', key=worker_id, value=json.dumps(serializable_discoveries))
        producer.flush()
    except Exception as e:
        # logging.error("Error reporting results for worker %s: %s", worker_id, e)
        print(f"Error reporting results for worker {worker_id}: {e}")

def report_completion(producer, worker_id):
    try:
        producer.produce('results-topic', key=worker_id, value=json.dumps({'status': 'completed'}))
        producer.flush()
    except Exception as e:
        # logging.error("Error reporting completion for worker %s: %s", worker_id, e)
        print(f"Error reporting completion for worker {worker_id}: {e}")

def fetch_work(consumer, worker_id):
    # logging.debug("Fetching work for worker %s", worker_id)
    consumer.subscribe(['work-topic'])

    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # logging.error("Kafka error: %s", msg.error())
                raise KafkaException(msg.error())

            # Deserialize task index if necessary
            task_index = int(json.loads(msg.value().decode('utf-8')))
            consumer.commit()
            # logging.debug("Worker %s received selector %s", worker_id, task_index)
            return task_index
        except Exception as e:
            # logging.error("Error fetching work for worker %s: %s", worker_id, e)
            print(f"Error fetching work for worker {worker_id}: {e}")

class LocalThresholdMaintainer:
    def __init__(self):
        self.local_threshold = float('-inf')

    def update_threshold(self, new_threshold):
        # logging.debug("Updating local threshold to %s", new_threshold)
        self.local_threshold = max(self.local_threshold, new_threshold)

    def prune_non_promising(self, patterns):
        # logging.debug("Pruning non-promising patterns")
        return [p for p in patterns if p['quality'] >= self.local_threshold]

local_threshold_maintainer = LocalThresholdMaintainer()

def traverse_node(worker_id, task, lv1selector):
    try:
        # logging.debug("Worker %s processing %s", worker_id, lv1selector)
        search_algorithm = ParallelBFS(task, lv1selector)
        result = search_algorithm.execute()
        discoveries = result.to_descriptions()

        discoveries = local_threshold_maintainer.prune_non_promising(discoveries)

        # logging.debug("Worker %s found: %s", worker_id, discoveries)
        report_results(producer, worker_id, discoveries)
        report_completion(producer, worker_id)
    except Exception as e:
        # logging.error("Error in traverse_node for worker %s: %s", worker_id, e)
        pass

def main(worker_id):
    try:
        while True:
            task_index = fetch_work(consumer, worker_id)
            if task_index is None:
                continue
            
            lv1selector = level1_selectors[task_index]
            task = ps.SubgroupDiscoveryTask(
                data=data,
                target=target,
                search_space=search_space,
                result_set_size=10,
                depth=2,
                qf=ps.WRAccQF()
            )
            traverse_node(worker_id, task, lv1selector)
    except KeyboardInterrupt:
        # logging.info("Worker %s interrupted by user", worker_id)
        pass
    except Exception as e:
        # logging.error("Worker %s encountered an error: %s", worker_id, e)
        pass

if __name__ == '__main__':
    try:
        data = pd.read_csv(r"data/gene_test.csv")
        target = ps.BinaryTarget("survival_category", "long")
        search_space = ps.create_selectors(data, ignore=["survival_category", "overall survival follow-up time"])
        level1_selectors = list(chain.from_iterable(ps.StaticSpecializationOperator(search_space).search_space))

        producer = Producer({'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093'})
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093',
            'group.id': 'worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })

        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()

        @zk.DataWatch("/GlobalMinQualityTh")
        def watch_threshold(data, stat):
            if data:
                new_threshold = float(data.decode("utf-8"))
                local_threshold_maintainer.update_threshold(new_threshold)
                # logging.debug("Worker %s updated local threshold to %s", worker_id, new_threshold)

        import atexit
        atexit.register(zk.stop)

        main(sys.argv[1])
    except Exception as e:
        # logging.error("Error initializing worker: %s", e)
        print(f"Error: {e}")