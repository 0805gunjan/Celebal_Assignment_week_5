import logging
from sqlalchemy import create_engine, text
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("log_sample_assignment4.txt"),
        logging.StreamHandler()
    ]
)

# Define source and destination DB URLs
source_url = "mysql+pymysql://root:Gunjan08@localhost/college"
dest_url = "mysql+pymysql://root:Gunjan08@localhost/college_selective_columns"

# Define which tables and columns to copy
tables_to_copy = {
    "employee": ["EnrollmentNo", "FirstName", "City"],
    "customer": ["NAME", "ADDRESS"]
}

try:
    # Connect to both databases
    src_engine = create_engine(source_url)
    dest_engine = create_engine(dest_url)

    logging.info(" Connected to both source and destination databases.")

    for table, columns in tables_to_copy.items():
        logging.info(f" Processing table: {table} with columns: {columns}")
        col_str = ", ".join(columns)

        # Read data from source
        query = f"SELECT {col_str} FROM {table}"
        df = pd.read_sql(query, src_engine)

        # Write to destination
        df.to_sql(table, dest_engine, index=False, if_exists="replace")
        logging.info(f"Successfully copied: {table}")

except Exception as e:
    logging.error(" Error occurred during replication")
    logging.exception(e)
