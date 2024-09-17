# Task Scheduler Framework (task_scheduler.py)
import schedule
import time
import datetime
import subprocess
# Logging and Monitoring (task_scheduler.py)
import logging

# Define a function to execute a task
def execute_task(task_name, max_runtime, command):
    try:
        start_time = datetime.datetime.now()
        # Example: Run an external command as the task
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        process.communicate()
        exit_code = process.returncode
        end_time = datetime.datetime.now()
        runtime = (end_time - start_time).total_seconds() / 3600  # Calculate runtime in hours
        if runtime > max_runtime:
            print(f"Task '{task_name}' exceeded max runtime ({max_runtime} hours) and was terminated.")
            process.terminate()  # Terminate the task
        else:
            print(f"Task '{task_name}' completed in {runtime:.2f} hours.")
            if exit_code != 0:
                print(f"Task '{task_name}' encountered an error. Exit code: {exit_code}")
    except Exception as e:
        print(f"Error executing task '{task_name}': {str(e)}")


# Scheduling and Running Tasks (task_scheduler.py)
# Define tasks and their max runtimes (in hours) and commands
tasks = [
    {"name": "Task1", "max_runtime": 3, "command": r"python task1_script.py"},
    {"name": "Task2", "max_runtime": 6, "command": r"python task2_script.py"},
    # Add more tasks here as needed
]


# Schedule tasks
for task in tasks:
    schedule.every().minute.do(execute_task, task["name"], task["max_runtime"], task["command"])
# Main loop to run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)  # Check for scheduled tasks every minute




# Configure logging
logging.basicConfig(filename='task_scheduler.log', level=logging.INFO)
# ...
# Inside execute_task function:
def execute_task(task_name, max_runtime, command):
    try:
        start_time = datetime.datetime.now()
        # Example: Run an external command as the task
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        process.communicate()
        exit_code = process.returncode
        end_time = datetime.datetime.now()
        runtime = (end_time - start_time).total_seconds() / 3600  # Calculate runtime in hours
        if runtime > max_runtime:
            logging.warning(f"Task '{task_name}' exceeded max runtime ({max_runtime} hours) and was terminated.")
            process.terminate()  # Terminate the task
        else:
            logging.info(f"Task '{task_name}' completed in {runtime:.2f} hours.")
            if exit_code != 0:
                logging.error(f"Task '{task_name}' encountered an error. Exit code: {exit_code}")
    except Exception as e:
        logging.error(f"Error executing task '{task_name}': {str(e)}")