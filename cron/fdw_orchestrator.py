import subprocess
import boto3
import sys
import os

# Initialize the Boto3 EC2 client
ec2_client = boto3.client('ec2', region_name='ap-south-1')

# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate up one level to reach the parent directory
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))

# Assuming virtual environment is located in the 'venv' directory within the parent directory
virtual_env_python = os.path.join(parent_dir, "venv", "bin", "python3")



def execute_script(script_path):
    try:
        # Path to the script you want to run
        script_path = os.path.join(parent_dir, f"{script_path}")

        #subprocess.run(["/usr/bin/python3", script_path], check=True) /home/ec2-user/FinancialDataWarehouse/venv/bin
        subprocess.run([virtual_env_python, script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        exit(1)


def main():
    # Execute data_load_equity_list.py
    execute_script("data_loads/data_load_equity_list.py")

    # Execute script2.py upon successful completion of script1.py
    #execute_script("/path/to/script2.py")

    # Execute script3.py upon successful completion of script2.py
    #execute_script("/path/to/script3.py")

    # Execute script4.py upon successful completion of script3.py
    #execute_script("/path/to/script4.py")


# Stop EC2 Instance
def stop_instance(instance_id):
    try:
        # Stop the EC2 instance
        ec2_client.stop_instances(InstanceIds=[instance_id])
        print(f"Instance {instance_id} stopped successfully.")
    except Exception as e:
        print(f"Error stopping instance {instance_id}: {e}")

if __name__ == "__main__":
    main()
    stop_instance("i-0c63c775026f5c981")