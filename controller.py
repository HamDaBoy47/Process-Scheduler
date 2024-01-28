import os 
import numpy as np
import json

# Load the configuration
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Schedulers to be tested
schedulers = ["mlfq", "fcfs", "stcf", "template"]

# Print the dataset info
print(f"\nDataset size : {config['dataset']}\n")
with open(f"Process_List/{config['dataset']}/dataset_info.txt", 'r') as f:
    print(f.read())
    f.close()
print("\n\n")

# Get the scheduler data files 
data = [f"data_" + str(i) + ".txt" for i in range(config['dataset-sizes'][config['dataset']])]

marker_data = {}  # {scheduler: [turnaround, response, burst, interrupt, score]}
wrong_schedulers = []

# Run all the schedulers
for scheduler in schedulers:

    sch_metrics = np.zeros(5)    

    # Run the scheduler for each data file
    for file in data:
        # Run the template scheduler
        if scheduler == "template":
            os.system("python Schedulers/template.py " + file)

        # Run the marker on the schedulers
        out = scheduler + '-' + file + '-' + os.popen("python marker.py " + file + " " + scheduler).read().strip()
        out_data = out.split('-')

        # Check if the scheduler is correct
        if out_data[2] == '':
            wrong_schedulers.append(scheduler)
            continue
        else:
            metrics = np.array(out.split('-')[2].split(','), dtype=float)
            sch_metrics += metrics
    
    # Calculate the average metrics
    marker_data[scheduler] = sch_metrics / len(data)


# Sort the schedulers based on the score
sorted_marker_data = sorted(marker_data.items(), key=lambda x: x[1][-1], reverse=False)
wrong_schedulers = list(set(wrong_schedulers))

# Print the metrics and the incorrect schedulers
print('| {:10} | {:10} | {:8} | {:8} | {:10} | {:7} |'.format("Scheduler", "Turnaround", "Response", "Burst", "Interrupt", "Score"))
print('---------------------------------------------------------------------------')
for scheduler in sorted_marker_data:
    if scheduler[0] in wrong_schedulers:
        continue
    metrics = np.round(scheduler[1], 2)
    print('| {:10} | {:10} | {:8} | {:8} | {:10} | {:7} |'.format(scheduler[0], metrics[0], metrics[1], metrics[2], metrics[3], metrics[4]))
if len(wrong_schedulers) > 0:
    print("\n")
    print("Incorrect Schedulers: ")
    print(wrong_schedulers)
  



