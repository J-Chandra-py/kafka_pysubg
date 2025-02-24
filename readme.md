1. Start Kafka and Zookeeper: 
Run the kafka cluster with only n brokers
```console
docker-compose up
```

2. Run the Master Node: 
Launch the master node script in one terminal:
```console
python master.py
```

3. Run Worker Nodes: 
Launch multiple instances of the worker node script:
syntax: python worker.py worker_id server:port
```console
syntax: python worker.py worker_id server:port
python worker.py worker1 port1
python worker.py worker2 port2
```