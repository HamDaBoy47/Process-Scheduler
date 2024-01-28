import numpy as np
import sys
import json

assert len(sys.argv) == 3, "Please provide the input file name and data name as a command line argument"

# Get the scheduler file data and the scheduler name
scheduler_data = sys.argv[1]
scheduler_name = sys.argv[2]

# Get the scheduler file number
file_num = scheduler_data.split("_")[1]

# Read in the config file 
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Read in the scheduler file data and the output file
with open('Schedulers/' + scheduler_name + '/' + config['dataset'] + '/' +scheduler_name + '_out_' + file_num, 'r') as o,\
      open('Process_List/' + config['dataset'] + '/' + scheduler_data, 'r') as d:
    data = d.read()
    out = o.read()
    d.close()
    o.close()

# Split the output into a list of processes
user_output = np.array(out.split(' '))
user_output = user_output[user_output != '']

# Get the requirement's data
data = np.array(data.split('\n'))
data = data[1:]
data = data[data != '']

# Save the requirement's data into process_list_data as a list
process_list_data = []
for i in data:
    splitData = i.split(",")
    process_list_data.append(splitData)

# Get the configuation for the dataset and the scheduler file data and the scheduler name
assertion_data = config['dataset'] + " " + scheduler_data + " " + scheduler_name + " "

# Get the unique processes from the user output
unique_process = np.unique(user_output)

# Re move the ! from the unique process
unique_raw_process = [element.strip('!') for element in unique_process]

# Get the unique processes from the process_list_data
unique_part = np.unique(unique_raw_process)

# Check if the user output has the all different processes processed by the scheduler
if len(unique_part) != len(process_list_data):
    error_ouput =  assertion_data + "All different processes are required to be processed by the scheduler\n"

# Get the process length of the data and the total process length from the scheduler file data
process_length = np.array(process_list_data)[:, 1].astype(int)
process_sum = np.sum(process_length)

# Define the error output
error_ouput = ""

# Check if the user output has the correct number of processes
if len(user_output) < process_sum:
    error_ouput += assertion_data + f"Expected {process_sum} processes but got {len(user_output)} processes\n"
else:  
    for process in process_list_data:
        # Get the process name and the IO name
        process_name = process[0]
        io_name = "!" +process_name

        # Get the process and IO occurances
        occurances = np.where(user_output == process_name)[0]
        IO_occurances = np.where(user_output == io_name)[0]
        
        # Get the arrival time
        arrival_time = int(process[2])
        
        # Get the process amounts and the IO amounts
        process_count = len(occurances)
        io_count = len(IO_occurances)
        
        # Check the process got require IO time:
        # IO not required, need the count to be 0 
        # Check process runs for correct time and has correct io:
        if process[3] == '0':
            if io_count != 0:
                error_ouput = assertion_data + f"Expected {IO_occurances} IO occurances for process {process_name} but got {io_count}"
                break
        else:
            # Get the expected IO amount
            expect_io_count = int(process[1])//int(process[3])
            if int(process[1])//int(process[3]) == int(process[1])/int(process[3]):
                expect_io_count -= 1

            if expect_io_count != io_count:
                error_ouput = assertion_data + "Expected {} IO occurances for process {} but\
                      got {}".format(expect_io_count, process_name, io_count)
                break
        
        # Check the process got the correct arrival time and ran for the correct time
        if arrival_time > int(occurances[0]):
            error_ouput = assertion_data + "Expected arrival time of process {} to be greater than or equal \
                to {} but got {}".format(process_name, arrival_time, int(occurances[0]))
            break

        if process_count != int(process[1]):
            error_ouput = assertion_data + "Expected process {} to run for {} time units but \
                got {}".format(process_name, int(process[1]), process_count)
            break

# Check if the error output is not empty
if error_ouput != "":
    # Write the error output to the error file 
    with open("Schedulers/" + str(scheduler_name) + "_error.txt", 'w') as f:
        f.write(error_ouput)
        f.close()
    print("")
else:
    # Calculate the metrics and the score   
    metrics = []
    for process in process_list_data:
        # Get the process name and the process occurances
        process_name = process[0]
        occurances = np.where(user_output == process_name)[0]
        
        # Get the arrival time, first run time and the complete time
        arrival_time = int(process[2])
        first_run_time = occurances[0]
        complete_time = occurances[-1]
        
        # Calculate the turnaround, response and burst time
        turnaround = complete_time - arrival_time
        response = first_run_time - arrival_time
        burst = occurances[-1] - occurances[0]

        metrics.append([turnaround, response, burst])
    
    process_switch_time = 0

    # Calculate the process switch time
    for k in range(len(user_output) - 1):
        if user_output[k] != user_output[k+1]:
            process_switch_time += 1

    metrics = np.array(metrics)

    # Calculate the average metrics
    avg_metrics = np.mean(metrics, axis = 0).round(config["round-dp"])

    score = config["weightings"]["turnaround"] * avg_metrics[0] + \
            config["weightings"]["response"] * avg_metrics[1] + \
            config["weightings"]["burst"] * avg_metrics[2] + \
            config["weightings"]["switch"] * process_switch_time


    print('{},{},{},{},{}'.format(avg_metrics[0], avg_metrics[1], avg_metrics[2],  process_switch_time, score))





