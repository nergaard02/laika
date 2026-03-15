# INF-3203 Distributed Scheduler (Áika-inspired)

This project implements a distributed task scheduling system inspired by the **Áika architecture**.  
The system distributes image classification tasks across multiple worker nodes while ensuring:

- Fault tolerance
- Task consistency (each task is completed exactly once)
- Load balancing
- Scalability

The implementation was developed as part of the **INF-3203 Advanced Distributed Systems** course.

---

# System Architecture

The system consists of three main components:

### Cluster Controller
Responsible for:
- Cluster coordination
- Leader election (Raft)
- Task scheduling
- Replication and consensus

Only the **Raft leader** assigns tasks.

### Local Controller
Runs on each worker node and is responsible for:
- Managing worker agents
- Fetching tasks from the cluster leader
- Monitoring worker processes
- Reporting completed tasks

Some local controllers may run **0 workers** and act as **replica nodes** for failure recovery.

### Agent
Worker process responsible for executing tasks:
- Runs the image classification pipeline
- Reports results to the cluster controller

---

# Task Description

The workload consists of classifying **1.2 million images**. Each image is classified using a pretrained **GoogLeNet model**.

The results are organized into the following structure:
labeled-data/
├── yellow_parrot/
│ ├── img1.jpg
│ ├── img2.jpg
│
├── greyhound/
│ ├── img3.jpg
│ ├── img4.jpg



Images are **not duplicated**; symbolic links are created instead.

---

# Consensus Protocol

The cluster controllers use the **Raft consensus algorithm** to ensure:

- Consistent task scheduling
- Exactly-once task completion
- Leader-based coordination

Raft is responsible for:

- Leader election
- Log replication
- State machine consistency

---

# Failure Model

The system assumes a **fail-stop failure model**, meaning that nodes either:

- operate correctly
- crash completely

Failures handled by the system include:

- Worker node crashes
- Local controller crashes
- Leader controller crashes

Recovery mechanisms include:

- Task leases (TTL)
- Task rescheduling
- Replica node activation
- Raft leader election

---

# Requirements

The system was tested using **Python 3.12**.

Install dependencies using:

```bash
pip install -r requirements.txt
```

# Running the System
The system is designed to run on the IFI cluster environment

## Start the cluster

Run the deployment script:

```bash
./run.sh
```

Default configuration:

- Cluster controllers: 3
- Local controllers: 2
- Replica controllers: 1
- Agents per node: 2

Custom configurations can be provided as arguments:

```bash
./run.sh <cluster_controllers> <local_controllers> <replicas> <agents_per_node>
```

Example
```bash
./run.sh 3 4 1 2
```

This starts:
- 3 cluster controllers
- 4 worker nodes
- 1 replica node
- 2 agents per worker node

##  Stop the cluster
To terminare all running processes and cleaning the environment:
```bash
./cleanup.sh
```

This script will:
- stop cluster controllers
- stop local controllers
- stop worker agents
- remove log files
- remove temporary Raft state files
- delete generated labeled data

# Running the Scalability Experiment
The scalability of the system is evaluated using the experiment script:
```bash
experiments/scalability





