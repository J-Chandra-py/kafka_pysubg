# ParaDis X pysubgroup-BFS

This project is an attempt to implement the ParaDis algorithm for subgroup discovery as discussed in the paper:
**"ParaDis: A parallel and distributed algorithm for subgroup discovery"**  

The implementation uses Kafka-based distributed messaging and the `pysubgroup` library to perform subgroup discovery tasks. The system consists of a master node and multiple worker nodes that collaborate to execute the algorithm.

## ⚠️ Warning: Project Status

This project is still under development and is not yet fully functional. The current implementation may not work as expected and is open for discussion, improvements, and contributions.

## Prerequisites

- Python 3.6+
- Docker and Docker Compose
- Required Python packages (listed in `requirements.txt`)

## Setup

1. **Start Kafka and Zookeeper using Docker Compose:**

   Use the provided `docker-compose.yml` file to start Kafka and Zookeeper:

   ```sh
   docker-compose up -d
   ```

   This will start Kafka and Zookeeper services in the background.

2. **Install Python packages:**

   Navigate to the project directory and install the required packages:

   ```sh
   pip install -r requirements.txt
   ```

## Running the Project

1. **Start the Master Node:**

   ```sh
   python src/master_m3.py
   ```

2. **Start Worker Nodes:**

   Open multiple terminal windows and start worker nodes with different IDs:

   ```sh
   python src/worker_m3.py <worker_id>
   ```

   Replace `<worker_id>` with a unique identifier for each worker (e.g., 1, 2, 3).

## Project Structure

- `ParaDis_pysubg/src/master_m3.py`: Master node script that distributes tasks and aggregates results.
- `ParaDis_pysubg/src/worker_m3.py`: Worker node script that processes tasks and reports results.
- `ParaDis_pysubg/requirements.txt`: List of required Python packages.
- `ParaDis_pysubg/docker-compose.yml`: Docker Compose file for setting up Kafka and Zookeeper.
- `ParaDis_pysubg/data/gene_test.csv`: Sample data file used for subgroup discovery.

## Configuration

- Kafka and Zookeeper configurations are specified in the `docker-compose.yml` file.
- Kafka topics used:
  - `work-topic`: For distributing tasks to workers.
  - `results-topic`: For collecting results from workers.
  - `threshold-topic`: For notifying workers of global threshold updates.

## Notes

- Ensure that Kafka and Zookeeper are running using Docker Compose before starting the master and worker nodes.
- Adjust the number of worker nodes based on the available resources and the size of the task.
- The project is still under development and may not work as expected. Contributions and discussions are welcome.

## Citations

1. **ParaDis Algorithm:**
   > **ParaDis: A parallel and distributed algorithm for subgroup discovery**  
   > Authors: A. K. Sharma, S. K. Gupta, and S. K. Singh  
   > Published in: Knowledge-Based Systems, Volume 266, 2023, Article 111335  
   > DOI: [10.1016/j.knosys.2023.111335](https://doi.org/10.1016/j.knosys.2023.111335)

2. **pysubgroup Library:**
   > This project uses the `pysubgroup` library for subgroup discovery.  
   > GitHub Repository: [flemmerich/pysubgroup](https://github.com/flemmerich/pysubgroup)
