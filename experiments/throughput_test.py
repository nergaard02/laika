import requests
import time
import statistics
import sys
import subprocess
import csv
import os
import matplotlib.pyplot as plt

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

sys.path.append(PROJECT_DIR)

from tests import load_cluster_info, stop_cluster, find_leader 

RUN_SCRIPT = os.path.join(PROJECT_DIR, "run.sh")

def start_cluster(cluster, locals, replicas, workers):
    print("Starting cluster...\n")

    subprocess.run(
        ["bash", RUN_SCRIPT, str(cluster), str(locals), str(replicas), str(workers)]
    )
    print("Started cluster")
    
    time.sleep(15)
    return

def experiment():
    
    configs = [
        (3,1,1,1),
        (3,2,1,1),
        (3,4,1,1),
        (3,8,1,1)
    ]
    
    epochs = 3
    results = {}
    
    for clusters, locals, replicas, workers in configs:
        total_workers = locals * workers
        results[total_workers] = []
        
        print(f"Current configuration: cluster controllers: {clusters}, local controllers: {locals}")
        
        for r in range(epochs):
            
            print(f"number epoch: {r}")
        
            start_cluster(clusters, locals, replicas, workers)
            
            cluster_info = load_cluster_info()
            cluster_controllers = cluster_info["cluster_controllers"]
            
            leader = find_leader(cluster_controllers)
            
            if not leader:
                print("Could not find leader")
                stop_cluster()
                return
            
            result = measure_throughput(leader)
            
            results[total_workers].append(result)
            
            stop_cluster()
            time.sleep(10)
    
    save_results(results)
    plot_results(results)

def measure_throughput(leader):
    runtime = 120 # seconds
    
    node, port = leader.split(":")
    status_url = f"http://{node}:{port}/status"
    
    print("warmup phase")
    time.sleep(30)
    
    start_data = requests.get(status_url, timeout=5).json()
    start_tasks_done = start_data["task_counts"]["DONE"]
    
    print("Starting measurement")
    start_time = time.time()
    
    time.sleep(runtime)
    
    end_data = requests.get(status_url).json()
    end_tasks_done = end_data["task_counts"]["DONE"]
    
    end_time = time.time()
    
    delta_tasks = end_tasks_done - start_tasks_done
    delta_time = end_time - start_time
    
    throughput = delta_tasks / delta_time
    return throughput


def save_results(results):
    with open("experiments/throughput_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["workers", "run", "throughput"])
        
        for workers, values in results.items():
            for i, v in enumerate(values):
                writer.writerow([workers, i + 1, v])
                
def plot_results(results):
    workers = []
    means = []
    stds = []
    
    for w, values in sorted(results.items()):
        workers.append(w)
        
        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 0
        
        means.append(mean)
        stds.append(std)
    
    plt.errorbar(workers, means, yerr=stds, capsize=5)
    
    plt.xlabel("Num workers")
    plt.ylabel("Throughput (tasks/sec)")
    plt.title("Scheduler Scalability")
    
    plt.savefig("experiments/scalability_plot.pdf")
    plt.savefig("experiments/scalability_plot.svg")
    
    plt.show()

if __name__ == "__main__":
    experiment()
        
        