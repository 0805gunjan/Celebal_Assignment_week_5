from sqlalchemy import create_engine
import pandas as pd
import datetime
import os
import logging
from fastavro import writer, parse_schema

# Setup logging
log_path = "C:/Users/DELL/Desktop/export_debug.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)

try:
    logging.info("Script started")

    output_dir = "C:/Users/DELL/Desktop/ExportOutput/"
    os.makedirs(output_dir, exist_ok=True)

    # ✅ Use SQLAlchemy to connect to MySQL
    engine = create_engine("mysql+pymysql://root:Gunjan08@localhost:3306/college")

    logging.info(" Connected to MySQL using SQLAlchemy")

    # ✅ Read student table
    query = "SELECT * FROM student"
    df = pd.read_sql(query, engine)
    logging.info(" Data fetched from student table")

    # ✅ CSV export
    csv_path = os.path.join(output_dir, "student.csv")
    df.to_csv(csv_path, index=False)
    logging.info(f" CSV saved to {csv_path}")

    # ✅ Parquet export
    parquet_path = os.path.join(output_dir, "student.parquet")
    df.to_parquet(parquet_path, engine="pyarrow", index=False)
    logging.info(f" Parquet saved to {parquet_path}")

    # ✅ Avro export
    avro_fields = []
    for col, dtype in zip(df.columns, df.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            avro_type = ["null", "int"]
        elif pd.api.types.is_float_dtype(dtype):
            avro_type = ["null", "float"]
        elif pd.api.types.is_bool_dtype(dtype):
            avro_type = ["null", "boolean"]
        elif pd.api.types.is_datetime64_any_dtype(dtype) or col.lower() == "bday":
            avro_type = ["null", {"type": "int", "logicalType": "date"}]
        else:
            avro_type = ["null", "string"]

        avro_fields.append({
            "name": col,
            "type": avro_type,
            "default": None
        })

    schema = {
        "name": "Student",
        "type": "record",
        "fields": avro_fields
    }

    parsed_schema = parse_schema(schema)
    records = df.to_dict(orient="records")

    for rec in records:
        for col in rec:
            val = rec[col]
            if isinstance(val, (datetime.date, datetime.datetime)):
                date_val = val.date() if isinstance(val, datetime.datetime) else val
                rec[col] = (date_val - datetime.date(1970, 1, 1)).days
            elif pd.isna(val):
                rec[col] = None
            else:
                rec[col] = val

    avro_path = os.path.join(output_dir, "student.avro")
    with open(avro_path, "wb") as out_file:
        writer(out_file, parsed_schema, records)

    logging.info(f" Avro saved to {avro_path}")
    logging.info(" Export script finished successfully")

except Exception as e:
    logging.error(" Something went wrong:")
    logging.exception(e)

