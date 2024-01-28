# OS Scheduler Assignment 

## Introduction
Your goal for this assignemnt is to design and implement a scheduler. A template file has been provided to make setup easier. 

## Setup & Run Instructions
Please ensure you have numpy installed in your environment. Additionally, make sure to set your working directory to the `Student_Handout` folder (the outer most folder).  

To run your program, please run the `controller.py` file located in the outer most file. This will run your program and rank it against the built in MLFQ, STCF, and FCFS schedulers.  

Please put all your code in the template.py file. Any additional classes or functions needed should be contained within this file.  

If your program generates any errors, they will be placed in the `Schedulers/template_error.txt` file.  

Additionally, your programs output for shceduling will be stored in the `Schedulers/template/` folder if you need to manually check it.

## Config 
A config file is provided so you can tweak the weightings (if needed) and also change the dataset that the controller marks on. 3 Datasets have been provided for you :  
* **Small**:  
  * 10 Process files
  * Min and max process time of 30 and 50 respectively
  * Min and max number of processes to 30 and 50 respectively   
  
* **Medium**:  
  * 100 Process files
  * Min and max process time of 30 and 150 respectively
  * Min and max number of processes to 80 and 150 respectively 

* **Large**:  
  * 150 Process files
  * Min and max process time of 150 and 350 respectively
  * Min and max number of processes to 150 and 350 respectively 

## Input / Output
The input and output has been handled for you in the template file. However, your algorithm should save its output to the string  `output` variable as describe in the `template.py` file. The format is `PROCESS` followed by a white space. If IO is required for that process, it is formatted as `!PROCESS` followed by a white space. Please use the output from the built in schedulers as a guide for formatting, found in `Schedulers/`

