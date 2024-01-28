import sys
import json
import heapq
from collections import deque

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Define the process data clas
class Process:
    def __init__(self, name, duration, arrival_time, io_frequency):
        self.name = name
        self.duration = duration
        self.arrival_time = arrival_time
        self.io_frequency = io_frequency
        self.io_check = False
        self.steps = 0
    
    def __lt__(self, other):
        # Custom comparison method for the heap
        return (self.duration, id(self)) < (other.duration, id(other))

def dynamic_quantum_scheduler(processes, boost_threshold=2, base_quantum=2):
    current_time = 0
    output_fragments = []
    queues = [[] for _ in range(3)]
    quantum = [base_quantum, 2 * base_quantum, float('inf')]
    boost_counter = 0

    while processes or any(queues):
        scheduled = False

        while processes and processes[0].arrival_time <= current_time:
            process = processes.pop(0)
            queues[0].append(process)

        if boost_counter == boost_threshold:
            boost_counter = 0
            if queues[2]:
                queues[0].extend(queues[2])
                queues[2] = []

        for i in range(len(queues)):
            if queues[i]:
                process = heapq.heappop(queues[i])

                # Dynamic Quantum Adjustment
                if process.steps > 0 and process.steps % base_quantum == 0:
                    quantum[i] = max(base_quantum, quantum[i] - 1)

                for _ in range(min(quantum[i], process.duration)):
                    if process.io_frequency == 0:
                        output_fragments.append(process.name)
                        process.duration -= 1
                    elif process.io_frequency > 0:
                        if process.steps == 0:
                            process.steps = 1
                            output_fragments.append(process.name)
                            process.duration -= 1

                        io_check_condition = process.steps % process.io_frequency == 0 and not process.io_check

                        if io_check_condition:
                            output_fragments.append(f"!{process.name}")
                            process.io_check = True
                        else:
                            output_fragments.append(process.name)
                        
                        if not io_check_condition:
                            process.io_check = False
                            process.duration -= 1
                            process.steps += 1
                            
                    if process.duration > 0:
                        if i < len(queues) - 1:
                            heapq.heappush(queues[i + 1], process)
                        else:
                            heapq.heappush(queues[i], process)

                scheduled = True
                break

        if not scheduled:
            output_fragments.append("")  # If no process was scheduled, append an empty string.

        current_time += 1
        boost_counter += 1

    return ' '.join(output_fragments).strip()

def mlfq_scheduler(processes, boost_threshold=15):
    current_time = 0
    output = ""
    queues = [[] for _ in range(3)]  # Three priority levels for MLFQ
    quantum = [6, 15, float('inf')]  # Time quantum for each queue, with the last queue having infinite quantum
    boost_counter = 0

    # Iterate through each process
    while processes or any(queues):
        # Add arrived processes to the appropriate queue
        while processes and processes[0].arrival_time <= current_time:
            process = processes.pop(0)
            queues[0].append(process)  # Add to the highest priority queue

        # Check if it's time for a CPU boost
        if boost_counter == boost_threshold:
            boost_counter = 0
            if queues[2]:
                # Move processes from the lowest priority queue to the highest
                queues[0].extend(queues[2])
                queues[2] = []

        # Iterate through queues from highest to lowest priority
        for i in range(len(queues)):
            if queues[i]:
                process = queues[i][0]
                queues[i] = queues[i][1:]  # Remove the process from the queue

                # Schedule the process
                for _ in range(min(quantum[i], process.duration)):
                    if process.io_frequency == 0:
                        output += f"{process.name} "
                        process.duration -= 1
                    elif process.io_frequency > 0:
                        if process.steps == 0:
                            process.steps = 1
                            output += f"{process.name} "
                            process.duration -= 1

                        # Check if it's time for IO
                        if process.steps % process.io_frequency == 0 and not process.io_check:
                            output += f"!{process.name} "
                            process.io_check = True
                        else:
                            process.steps += 1
                            output += f"{process.name} "
                            process.duration -= 1
                            process.io_check = False

                    # Add the process back to a lower priority queue if it's not completed
                    if process.duration > 0:
                        if i < len(queues) - 1:
                            queues[i + 1].append(process)  # Move to the next lower priority queue
                        else:
                            queues[i].append(process)  # Stay in the lowest priority queue

                break  # Move to the next time step

        # If no process was scheduled, time passes
        current_time += 1
        boost_counter += 1

    return output.strip()

def stcf_scheduler(processes):
    current_time = 0
    output = ""
    process_queue = []

    # Iterate through each process
    while processes or process_queue:
        # Add arrived processes to the queue
        while processes and processes[0].arrival_time <= current_time:
            heapq.heappush(process_queue, processes.pop(0))

        if not process_queue:
            current_time += 1
            continue

        # Get the process with the shortest remaining execution time
        process = heapq.heappop(process_queue)

        # Schedule the process
        if process.io_frequency == 0:
            output += f"{process.name} "
            process.duration -= 1
        elif process.io_frequency > 0:
            if process.steps == 0:
                process.steps = 1
                output += f"{process.name} "
                process.duration -= 1

            # Check if it's time for IO
            if process.steps % process.io_frequency == 0 and not process.io_check:
                output += f"!{process.name} "
                process.io_check = True
            else:
                process.steps += 1
                output += f"{process.name} "
                process.duration -= 1
                process.io_check = False

        current_time += 1

        # Add the process back to the queue if it's not completed
        if process.duration > 0:
            heapq.heappush(process_queue, process)

    return output.strip()

def fcfs_scheduler(processes, num_processes):
    current_time = 0
    output = ""

    # Iterate through each process
    while num_processes > 0:
        process = min(processes, key=lambda p:p.arrival_time)

        # Check if the process has arrived
        if process.arrival_time > current_time:
            current_time += 1
        else:
            # print(f"Process {process.name} has arrived at time {current_time}")
            while (process.duration != 0):
                # Schedule the process
                if process.io_frequency == 0:
                    output += f"{process.name} "
                    process.duration -= 1
                # Check for IO requests
                elif process.io_frequency > 0:
                    if process.steps == 0:
                        process.steps = 1
                        output += f"{process.name} "
                        process.duration -= 1

                    # Check if it's time for IO
                    if process.steps % process.io_frequency == 0 and process.io_check == False:
                        output += f"!{process.name} "
                        process.io_check = True
                    else:
                        process.steps += 1
                        output += f"{process.name} "
                        process.duration -= 1
                        process.io_check = False

                current_time += 1

            processes.remove(process)
            num_processes -= 1

    return output
                    
def main():
    # Check if the correct number of arguments is provided
    import sys
    if len(sys.argv) != 2:
        return 1

    # Extract the input file name from the command line arguments
    input_file_name = f"Process_List/{config['dataset']}/{sys.argv[1]}"

    # Define the number of processes
    num_processes = 0

    # Initialize an empty list for process data
    data_set = []

    # Open the file for reading
    try:
        with open(input_file_name, "r") as file:
            # Read the number of processes from the file
            num_processes = int(file.readline().strip())

            # Read process data from the file and populate the data_set list
            for _ in range(num_processes):
                line = file.readline().strip()
                name, duration, arrival_time, io_frequency = line.split(',')
                process = Process(name, int(duration), int(arrival_time), int(io_frequency))
                data_set.append(process)

    except FileNotFoundError:
        print("Error opening the file.")
        return 1

    """
    TODO Your Algorithm - assign your output to the output variable
    """
    
    # Run the FCFS scheduler
    # output = stcf_scheduler(data_set, num_processes)
    output = dynamic_quantum_scheduler(data_set)
    
    """
    End of your algorithm
    """

    # Open a file for writing
    try:
        output_path = f"Schedulers/template/{config['dataset']}/template_out_{sys.argv[1].split('_')[1]}"
        with open(output_path, "w") as output_file:
            # Write the final result to the output file
            output_file.write(output)

    except IOError:
        print("Error opening the output file.")
        return 1

    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
