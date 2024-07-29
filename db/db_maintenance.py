import os
import sys

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from db.database_and_models import text
from sqlalchemy import create_engine
from datetime import datetime
import logging
from configs.config import DATABASE_URL

#Logs
# Create a timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Configure the logging settings
#log_filename = f"../logs/db_maintenance_{timestamp}.log"
log_filename = os.path.join(parent_dir, f"logs/db_maintenance_{timestamp}.log")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='w'
)

# Create a logger object
logger = logging.getLogger(__name__)

# Create a new SQLAlchemy engine instance
engine_db_maint = create_engine(DATABASE_URL)

sql_vacuum_analyze = text("VACUUM FULL ANALYZE;")
try:
    with engine_db_maint.connect() as connection:
        # Enable autocommit mode to run VACUUM outside a transaction block
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        # Execute the VACUUM FULL ANALYZE command
        connection.execute(sql_vacuum_analyze)
except Exception as e:
    logger.error(f'db_maintenance(VACUUM FULL ANALYZE) - Exception : {e}')
else:
    # If no exceptions occur, commit the transaction
    # session.commit()
    logger.info(f'db_maintenance() - VACUUM FULL and ANALYZE completed successfully.\n')