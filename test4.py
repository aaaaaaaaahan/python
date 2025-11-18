import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging

# Set up logging
logging.basicConfig(
    filename='data_cleaner.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_parquet_file(file_path, output_folder):
    try:
        # Read Parquet file
        df = pd.read_parquet(file_path)
        total_rows = len(df)
        skipped_rows = 0

        # Keep only rows that can convert to string safely
        for col in df.select_dtypes(include='object').columns:
            valid_mask = df[col].apply(lambda x: isinstance(x, str) or pd.isna(x))
            skipped_rows += (~valid_mask).sum()
            if (~valid_mask).any():
                logging.warning(f"{file_path}: { (~valid_mask).sum() } invalid rows in column '{col}' skipped.")
            df = df[valid_mask | pd.isna(df[col])]

        # Save cleaned Parquet
        output_path = os.path.join(output_folder, os.path.basename(file_path))
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)

        logging.info(f"{file_path}: Processed {len(df)} rows, skipped {skipped_rows} rows.")
        print(f"Processed {file_path}, skipped {skipped_rows} rows.")

    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")

def process_folder(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(input_folder, file_name)
            process_parquet_file(file_path, output_folder)

# Example usage
input_folder = 'sas_converted_parquet'
output_folder = 'cleaned_parquet'
process_folder(input_folder, output_folder)
