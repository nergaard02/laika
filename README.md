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
experiments/throughput_test.py
```
This experiment automatically:
1. Starts the cluster
2. Detects the current Raft leader
3. Runs the workload for a fixed time interval
4. Measures task throughput
5. Repeats the  test with different number of worker nodes
6. Generates performance results

## Experiment output
The experiment produces the following output files:
- throughput_results.csv
- scalability_plot.pdf
- scalability_plot.svg

These files contain:
- raw throughput measurements
- average throughput values
- a scalability plot showing workers vs throughput

The generated plot includes error bars representing standard deviation across multiple runs

# Testing

The project also includes a set of automated tests located in the `tests.py` file.  
These tests verify both the correctness of the task scheduler and the behaviour of the Raft consensus implementation.

The tests simulate various failure scenarios to ensure that the system behaves correctly in a hostile cluster environment.

## Types of Tests

### State Machine Tests

These tests verify that the scheduler state machine enforces correct task semantics.

Example checks include:

- tasks cannot be completed twice
- results from outdated workers are ignored
- rescheduled tasks are assigned correctly

Example test:

- `test_duplicate_task()` ensures that when a task is reassigned after lease expiration, results from the old worker are ignored.

---

### Cluster Failure Tests

These tests simulate failures of worker nodes and local controllers.

Examples include:

- `test_replica_activation()`  
  Verifies that a replica node is activated, and thus used for recivovery, when another local controller node crashes.

- `test_node_rejoin_after_crash()`  
  Ensures that a local controller node that crashes and restarts successfully re-registers with the cluster.

---

### Raft Consensus Tests

The Raft implementation is tested under several failure scenarios to verify correct consensus behaviour.

The following properties are tested:

- leader election after failure
- leader step-down when an old leader rejoins
- follower log catch-up after restart
- log conflict resolution
- outdated nodes cannot win elections

Example tests include:

- `test_leader_election_after_crash()`  
  Ensures that a new leader is elected when the current leader fails.

- `test_old_leader_steps_down_after_restart()`  
  Ensures that a restarted leader steps down when it discovers a newer term.

- `test_follower_log_catchup_after_restart()`  
  Verifies that followers correctly synchronize their logs after restarting.

- `test_log_conflict_resolution()`  
  Tests Raft's log conflict resolution mechanism.

- `test_outdated_log_cannot_win_election()`  
  Ensures that nodes with outdated logs cannot become leader.

---

## Running the Tests

Tests can be executed using **pytest**:

```bash
pytest tests.py
```

Note that some tests interact with the cluster and may take several seconds to complete due to node startup, crash simulation, and recovery.







