import subprocess
import datetime
import sys
import os

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from helper.custom_logging_script import setup_logger, custom_logging
from configs.config import ec2_client, ec2_istance_id, initialize_timestamp
from helper.custom_generate_email import email
from helper.helper_identify_holidays import is_holiday

# Define the script name here
script_name = os.path.basename(__file__).split('.')[0]

# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate up one level to reach the parent directory
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))

# Assuming virtual environment is located in the 'venv' directory within the parent directory
virtual_env_python = os.path.join(parent_dir, "venv", "bin", "python3")

# Stop EC2 Instance
def stop_instance(instance_id):
    try:
        # Stop the EC2 instance
        #ec2_client.stop_instances(InstanceIds=[instance_id]) # to be un-commented
        print(f"Instance {instance_id} stopped successfully.")
        custom_logging(logger, 'INFO', f"Instance {instance_id} stopped successfully.")
    except Exception as e:
        print(f"Error stopping instance {instance_id}: {e}")
        custom_logging(logger, 'ERROR', f"Error in stopping instance {instance_id}: {e}")


def execute_script(script_path):
    try:
        # Path to the script you want to run
        script_path = os.path.join(parent_dir, f"{script_path}")

        #subprocess.run(["/usr/bin/python3", script_path], check=True) /home/ec2-user/FinancialDataWarehouse/venv/bin
        subprocess.run([virtual_env_python, script_path], check=True)

        custom_logging(logger, 'INFO', f'Completed {script_path}')
    except subprocess.CalledProcessError as e:
        #print(f"Error executing {script_path}: {e}")
        #custom_logging(logger, 'ERROR', f"Error executing {script_path}: {e}")
        raise  # Reraise the exception so it can be caught in the main() function


def main():
    try:
        check_holiday = is_holiday()
        if not check_holiday.get('is_holiday'):
            # Execute generate_token.py
            execute_script("data_loads/generate_token.py")

            # Execute data_load_equity_list.py
            execute_script("data_loads/data_load_equity_list.py")

            #execute_script("data_loads/data_load_equity_daily.py")
        else:
            custom_logging(logger, 'INFO', f'Holiday Today due to {check_holiday.get("info")}')


    except Exception as e:
        print(f"Error in main(): {e}")
        custom_logging(logger, 'ERROR', f"Error in main(): {e}")
        perform_cleanup()
        exit(1)

def perform_cleanup():
    stop_instance(ec2_istance_id)
    email()

if __name__ == "__main__":
    initialize_timestamp() # For Global variable timestamp

    # Initialize the logger with the script name
    logger = setup_logger(script_name)

    custom_logging(logger, 'INFO', f'Start time: {datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}')
    main()
    perform_cleanup()
    custom_logging(logger, 'INFO', f'End time: {datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}')