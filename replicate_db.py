import pandas as pd
from sqlalchemy import create_engine, inspect, text
import logging

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === Database Config ===
SOURCE_DB_URI = 'mysql+pymysql://root:Gunjan08@localhost/college'
DEST_DB_URI   = 'mysql+pymysql://root:Gunjan08@localhost/destination_db'

# === Connect to Databases ===
source_engine = create_engine(SOURCE_DB_URI)
dest_engine = create_engine(DEST_DB_URI)

try:
    logging.info("Connected to both source and destination databases.")

    # === Get Tables ===
    inspector = inspect(source_engine)
    tables = inspector.get_table_names()
    logging.info(f"Found {len(tables)} tables to copy.")

    for table in tables:
        logging.info(f"Processing table: {table}")
        
        # Read schema (columns)
        columns_info = inspector.get_columns(table)
        col_defs = []
        for col in columns_info:
            col_type = str(col['type'])
            nullable = 'NULL' if col['nullable'] else 'NOT NULL'
            default = f"DEFAULT {col['default']}" if col['default'] is not None else ''
            col_defs.append(f"`{col['name']}` {col_type} {nullable} {default}")
        create_stmt = f"CREATE TABLE IF NOT EXISTS `{table}` ({', '.join(col_defs)});"
        # Create table in destination DB
        with dest_engine.connect() as conn:
            conn.execute(text(create_stmt))
            conn.commit()
        logging.info(f"Created table `{table}` in destination DB.")


        # Copy data
        df = pd.read_sql(f"SELECT * FROM `{table}`", source_engine)
        df.to_sql(name=table, con=dest_engine, if_exists='replace', index=False)
        logging.info(f"Copied {len(df)} rows to `{table}`.")

    logging.info("All tables copied successfully.")

except Exception as e:
    logging.error("Error occurred:")
    logging.exception(e)
