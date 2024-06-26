import os
import sys
import logging
from datetime import datetime

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

def setup_logger(script_name, process_start_time):
    # Add parent directory to the Python path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    # Create a timestamp string
    #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #print(f'logger get_timestamp() = {get_timestamp()}')

    # Configure the logging settings
    log_filename = os.path.join(parent_dir, f"logs/{script_name}_{process_start_time}.log")

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.ERROR)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_filename,
        filemode='w'
    )

    # Create a logger object
    logger = logging.getLogger(__name__)

    return logger

def custom_logging(logger, type='INFO', msg=''):
    if type == 'INFO':
        logger.info(msg)
    elif type == 'ERROR':
        logger.error(msg)
