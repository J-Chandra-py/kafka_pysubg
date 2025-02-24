from confluent_kafka import Producer, Consumer, KafkaException
from heapq import heappop, heappush

import json
import sys
# import time
import numpy as np
import pandas as pd

import pysubgroup as ps

producer_conf = {'bootstrap.servers': 'localhost:{server_port}'}
producer = Producer(**producer_conf)

consumer_conf = {
    f'bootstrap.servers': 'localhost:{server_port}',
    'group.id': 'worker-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)


class Parallel_BFS:
    def __init__(self, task, lv1selector):
        self.task = task
        self.lv1selector = lv1selector
        self.subroot = True
    
    def execute(self, task):
        result = []
        queue = [(float("-inf"), ps.Conjunction([]))]
        # queue = [(q, self.lv1selector)]
        operator = ps.StaticSpecializationOperator(task.search_space)
        task.qf.calculate_constant_statistics(task.data, task.target)
        while queue:
            q, old_description = heappop(queue)
            q = -q
            if not q > ps.minimum_required_quality(result, task):
                break
            if self.subroot:
                sGs = (old_description & self.lv1selector,)
                self.subroot = False
            else:
                sGs = operator.refinements(old_description)
            for candidate_description in sGs:
                sg = candidate_description
                statistics = task.qf.calculate_statistics(sg, task.target, task.data)
                ps.add_if_required(
                    result,
                    sg,
                    task.qf.evaluate(sg, task.target, task.data, statistics),
                    task,
                    statistics=statistics,
                )
                if len(candidate_description) < task.depth:
                    if hasattr(task.qf, "optimistic_estimate"):
                        optimistic_estimate = task.qf.optimistic_estimate(
                            sg, task.target, task.data, statistics
                        )
                    else:
                        optimistic_estimate = np.inf

                    # Compute refinements and fill the queue
                    if optimistic_estimate >= ps.minimum_required_quality(result, task):
                        if ps.constraints_satisfied(
                            task.constraints_monotone,
                            candidate_description,
                            statistics,
                            task.data,
                        ):
                            heappush(
                                queue, (-optimistic_estimate, candidate_description)
                            )

        result = ps.prepare_subgroup_discovery_result(result, task)
        return ps.SubgroupDiscoveryResult(result, task)

def delivery_report(err, msg):
    if err is not None:
        sys.stderr.write(f'Delivery failed for record {msg.key()}: {err}\n')
    else:
        sys.stdout.write(f'Record {msg.key()} successfully produced to {msg.topic()} at offset {msg.offset()}\n')


def report_discoveries(worker_id, discoveries):
    producer.produce(
        'results-topic', 
        key=worker_id, 
        value=json.dumps(discoveries), 
        callback=delivery_report
    )
    producer.flush()

def report_completion(producer, worker_id):
    producer.produce(
        'results-topic',
        key=worker_id,
        value=json.dumps({'status': 'completed'}),
        callback=delivery_report
    )
    producer.flush()

def fetch_work(worker_id):
    consumer.subscribe(['work-topic-'+worker_id])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                sys.stdout.write(f'Received selector to traverse: {msg.value().decode("utf-8")}\n')
                return json.loads(msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        sys.stderr.write('%% Keyboard Interrupt\n')
    finally:
        consumer.close()


def traverse_node(worker_id, task, lv1selector):
    sys.stdout.write(f'Worker {worker_id} is processing data...\n')
    
    
    search_algorithm = Parallel_BFS(task, lv1selector)
    result = search_algorithm.execute(task)

    discoveries = result.to_descriptions()
    report_discoveries(worker_id, discoveries)
    report_completion(producer, worker_id)

def main(worker_id):
    sys.stdout.write(f'Starting worker {worker_id}\n')
    lv1selector = fetch_work(worker_id)
    # Convert data into a task object for BestFirstSearch
    task = ps.SubgroupDiscoveryTask(data=data,
                                target=target,
                                search_space=search_space,
                                result_set_size=10,
                                depth=2,
                                qf=ps.WRAccQF(),
                                # constraints=[lv1selector]
                                )
    if lv1selector:
        traverse_node(worker_id, task, lv1selector) #usually take more time to traverse when the patterns to traverse under the node is deep


if __name__ == '__main__':
    data = pd.read_csv("C:\Projects\SoftwareLab_PreThesis\gene_test.csv")
    target = ps.BinaryTarget("survival_category", "long")
    search_space = ps.create_selectors(data, ignore=["survival_category", "overall survival follow-up time"])
    if len(sys.argv) > 1:
        main(sys.argv[1])
        server_port = sys.argv[2]
    else:
        sys.stderr.write('Worker ID is required\n')