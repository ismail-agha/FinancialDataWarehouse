import configs.config as config
import configs.config_urls as curls
import subprocess
import json

import time

while True:
    try:
        # Run the subprocess command and capture the output
        output = subprocess.check_output(curls.curl_cmd_bse_equity)

        # Convert the JSON string to a Python dictionary
        data = json.loads(output)

        # Print the data and its length


        # Check if the length of the data is more than 3999
        if len(data) > 3999:
            print(f'Length = {len(data)}. Done.')
            break
        else:
            print(f'Length = {len(data)}. Fetching Again.')

        # Wait for 5 seconds before the next iteration
        time.sleep(5)

    except subprocess.CalledProcessError as e:
        print(f"Subprocess error: {e.output}")
        break
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        break
    except Exception as e:
        print(f"Unexpected error: {e}")
        break