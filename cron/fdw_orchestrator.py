import subprocess

def execute_script(script_path):
    try:
        #subprocess.run(["/usr/bin/python3", script_path], check=True) /home/ec2-user/FinancialDataWarehouse/venv/bin
        subprocess.run(["/home/ec2-user/FinancialDataWarehouse/venv/bin", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        exit(1)


def main():
    # Execute data_load_equity_list.py
    execute_script("/data_loads/data_load_equity_list.py")

    # Execute script2.py upon successful completion of script1.py
    #execute_script("/path/to/script2.py")

    # Execute script3.py upon successful completion of script2.py
    #execute_script("/path/to/script3.py")

    # Execute script4.py upon successful completion of script3.py
    #execute_script("/path/to/script4.py")

    # Stop EC2 Instance

if __name__ == "__main__":
    main()