#!/usr/bin/env python3
"""
Parallelized ETL: process multiple input files in parallel (one file per process).
Each file still processes in chunks (Dask or local compute) and writes a single Parquet file.

Usage:
 - normal run (parallel processing across files):
     python etl_parallel.py
 - rerun only failed:
     python etl_parallel.py --rerun-failed
"""

import os
import ast
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
import mmap
import numpy as np
import time
import re
from typing import List, Tuple
from datetime import datetime, timedelta
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

# Optional: use numba if available
try:
    from numba import njit
    NUMBA_AVAILABLE = True
except Exception:
    NUMBA_AVAILABLE = False

# Dask
import dask
from dask import delayed
try:
    from dask.distributed import Client, as_completed as dask_as_completed
    DASK_DISTRIBUTED_AVAILABLE = True
except Exception:
    Client = None
    dask_as_completed = None
    DASK_DISTRIBUTED_AVAILABLE = False

# -------------------------
# CONFIG
# -------------------------
INPUT_FOLDER_PATHS = [
    "control"
]
VAR_PATH = "column_config"
OUTPUT_FOLDER_PATH = "sas_parquet"

# Tune these
CHUNK_RECORDS = 200_000
DASK_WORKERS = 4                # used if using dask.distributed inside a file
DASK_THREADS_PER_WORKER = 1

# New: number of files to process in parallel (process-per-file)
PARALLEL_FILES = 100

FAILED_FILE_LOG = "/host/cis/control/job_status/failed_files.txt"

# -------------------------
# Helpers
# -------------------------
def get_file_names(directory_path: str) -> List[Tuple[str, str, str]]:
    file_names = []
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        batch_files = []

        files = os.listdir(directory_path)
        for file in files:
            full_path = os.path.join(directory_path, file)
            if os.path.isfile(full_path):
                base_name = file.rsplit(".", 1)[0]

                match = re.match(r"(.+?)_(\d{8})$", base_name)
                if match:
                    name_part, date_str = match.groups()
                    try:
                        datetime.strptime(date_str, "%Y%m%d")
                        batch_files.append((file, name_part, date_str))
                    except ValueError:
                        continue

        if not batch_files:
            return []

        yesterday_files = [f for f in batch_files if f[2] == yesterday]
        if yesterday_files:
            return yesterday_files

        latest_date = max(f[2] for f in batch_files)
        latest_files = [f for f in batch_files if f[2] == latest_date]
        print(f"No files for yesterday. Falling back to latest batch date: {latest_date}")
        return latest_files

    except FileNotFoundError:
        print(f"Error: The directory '{directory_path}' was not found")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []


def load_variables(var_path: str, script_name: str):
    input_path = os.path.join(var_path, f"{script_name}_output.txt")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file '{input_path}' not found.")

    with open(input_path, 'r') as file:
        content = file.read()

    try:
        lrecl_start = content.index("Lrecl:") + len("Lrecl:")
        lrecl_end = content.index("Specs:")
        lrecl_str = content[lrecl_start:lrecl_end].strip()
        lrecl = int(lrecl_str)

        specs_start = content.index("Specs:") + len("Specs:")
        names_start = content.index("Names:") + len("Names:")
        other_start = content.index("Other:")

        specs_str = content[specs_start:names_start - + len("Names:")].strip()
        names_str = content[names_start:other_start].strip()
        other_str = content[other_start + len("Other:"):].strip()

        specs = ast.literal_eval(f"[{specs_str}]")
        names = ast.literal_eval(f"[{names_str}]")
        other = ast.literal_eval(f"[{other_str}]")

        column_types, column_subtypes, column_decimals = [], [], []
        for entry in other:
            parts = [x.strip() for x in entry.split(",")]
            if len(parts) < 3:
                parts += ["", "","0"]
            col_type, subtype, decimal = parts[:3]
            column_types.append(col_type)
            column_subtypes.append(subtype)
            column_decimals.append(int(decimal))
    except Exception as e:
        raise ValueError(f"Failed to parse variable file: {e}")

    return specs, names, other, column_types, column_subtypes, column_decimals, lrecl

def check_and_delete_table(output_file: str):
    if os.path.exists(output_file):
        print(f"Table {output_file} exists. Deleting...")
        os.remove(output_file)
        print(f"Deleted: {output_file}")

# -------------------------
# Validators & Schema
# -------------------------
def validate_specs(column_specs, column_names, lrecl):
    if len(column_specs) != len(column_names):
        raise ValueError("Specs / names length mismatch")
    for s,e in column_specs:
        if s < 0 or e < s or e >= lrecl:
            raise ValueError(f"Spec out of bounds: {(s,e)} vs lrecl={lrecl}")
    sorted_ranges = sorted(column_specs, key=lambda t: t[0])
    for a,b in zip(sorted_ranges, sorted_ranges[1:]):
        if a[1] >= b[0]:
            print("Warning: overlapping column specs:", a, b)

def build_target_schema(column_names, column_types):
    fields = []
    for name, ctype in zip(column_names, column_types):
        if ctype == "packed":
            typ = pa.float64()
        else:
            typ = pa.string()
        fields.append(pa.field(name, typ))
    return pa.schema(fields)

def write_chunk_df_with_schema(writer, df, target_schema):
    cols = [f.name for f in target_schema]
    df = df.reindex(columns=cols)

    for field in target_schema:
        if pa.types.is_string(field.type):
            df[field.name] = df[field.name].astype("string[pyarrow]")
        elif pa.types.is_floating(field.type):
            df[field.name] = pd.to_numeric(df[field.name], errors="coerce").astype("float64")

    table = pa.Table.from_pandas(df, schema=target_schema, preserve_index=False)
    writer.write_table(table)

# -------------------------
# Packed-decimal decode
# -------------------------
if NUMBA_AVAILABLE:
    @njit
    def _decode_s370fpd_numba_flat(field_bytes: np.ndarray, decimals: int) -> float:
        value = 0
        for b in field_bytes[:-1]:
            hi = b >> 4
            lo = b & 0x0F
            value = value * 100 + (hi * 10 + lo)
        last = field_bytes[-1]
        sign = last & 0x0F
        hi = (last >> 4) & 0x0F
        value = value * 10 + hi
        if sign == 0x0D or sign == 0x0B:
            value = -value
        return value / (10 ** decimals)

    @njit
    def decode_s370fpd_column_numba(data_block: np.ndarray, start: int, end: int, decimals: int) -> np.ndarray:
        n = data_block.shape[0]
        out = np.empty(n, dtype=np.float64)
        width = end - start + 1
        for i in range(n):
            field = data_block[i, start:start+width]
            out[i] = _decode_s370fpd_numba_flat(field, decimals)
        return out

def decode_s370fpd_column_numpy(data_block: np.ndarray, start: int, end: int, decimals: int) -> np.ndarray:
    n = data_block.shape[0]
    width = end - start + 1
    out = np.empty(n, dtype=np.float64)
    for i in range(n):
        field = data_block[i, start:start+width]
        value = 0
        for b in field[:-1]:
            hi = b >> 4
            lo = b & 0x0F
            value = value * 100 + (hi * 10 + lo)
        last = field[-1]
        sign = last & 0x0F
        hi = (last >> 4) & 0x0F
        value = value * 10 + hi
        if sign in (0x0D, 0x0B):
            value = -value
        out[i] = value / (10 ** decimals)
    return out

def decode_s370fpd_column(data_block: np.ndarray, start: int, end: int, decimals: int) -> np.ndarray:
    if NUMBA_AVAILABLE:
        return decode_s370fpd_column_numba(data_block, start, end, decimals)
    else:
        return decode_s370fpd_column_numpy(data_block, start, end, decimals)

# -------------------------
# Decoders
# -------------------------
def decode_ebcdic_column(data_block: np.ndarray, start: int, end: int) -> List[str]:
    nrows = data_block.shape[0]
    width = end - start + 1
    block = data_block[:, start:end+1]
    raw = block.tobytes()
    decoded_all = raw.decode("cp037", errors="ignore")
    result = [decoded_all[i*width:(i+1)*width].rstrip("\x00").strip() for i in range(nrows)]
    return [r if r != "" else None for r in result]

def decode_ascii_column(data_block: np.ndarray, start: int, end: int) -> List[str]:
    nrows = data_block.shape[0]
    width = end - start + 1
    block = data_block[:, start:end+1]
    raw = block.tobytes()
    decoded_all = raw.decode("ascii", errors="ignore")
    result = [decoded_all[i*width:(i+1)*width].strip() for i in range(nrows)]
    return [r if r != "" else None for r in result]

# -------------------------
# Binary loading & chunk processing
# -------------------------
def load_binary_slice(input_file: str, lrecl: int, start_rec: int, rec_count: int) -> np.ndarray:
    with open(input_file, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        start_byte = start_rec * lrecl
        end_byte = start_byte + rec_count * lrecl
        end_byte = min(end_byte, mm.size())
        slice_bytes = mm[start_byte:end_byte]
        actual_len = len(slice_bytes) - (len(slice_bytes) % lrecl)
        slice_bytes = slice_bytes[:actual_len]
        if actual_len == 0:
            mm.close()
            return np.empty((0, lrecl), dtype=np.uint8)
        arr = np.frombuffer(slice_bytes, dtype=np.uint8)
        arr = arr.reshape((-1, lrecl))
        mm.close()
        return arr

def process_binary_chunk(input_file: str, lrecl: int, start_rec: int, rec_count: int,
                         column_specs, column_names, column_types, column_subtypes, column_decimals) -> pd.DataFrame:
    data = load_binary_slice(input_file, lrecl, start_rec, rec_count)
    if data.shape[0] == 0:
        return pd.DataFrame({c: [] for c in column_names})

    results = {}
    for (start, end), name, ctype, subtype, decimal in zip(column_specs, column_names, column_types, column_subtypes, column_decimals):
        if ctype == "packed" and subtype == "s370fpd":
            arr = decode_s370fpd_column(data, start, end, decimal)
            results[name] = arr
        elif ctype == "ebcdic":
            results[name] = decode_ebcdic_column(data, start, end)
        else:
            results[name] = decode_ascii_column(data, start, end)
    return pd.DataFrame(results)

def process_text_file_chunks(input_file: str, column_specs, column_names, chunk_lines: int = 200_000):
    adj_specs = [(s, e+1) for s, e in column_specs]
    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        for chunk in pd.read_fwf(
            f,
            colspecs=adj_specs,
            names=column_names,
            chunksize=chunk_lines,
            dtype=str
        ):
            yield chunk

# -------------------------
# Task builder
# -------------------------
def build_tasks_for_file(input_file: str, column_specs, column_names, column_types, column_subtypes, column_decimals, lrecl):
    _, ext = os.path.splitext(input_file)
    ext = ext.lower()
    tasks = []
    if ext == ".txt":
        for chunk_df in process_text_file_chunks(input_file, column_specs, column_names, CHUNK_RECORDS):
            df_chunk = chunk_df.astype(object)
            tasks.append(delayed(lambda x: x)(df_chunk))
    else:
        filesize = os.path.getsize(input_file)
        total_records = filesize // lrecl
        if total_records == 0:
            return []
        rec = 0
        while rec < total_records:
            this_count = min(CHUNK_RECORDS, total_records - rec)
            task = delayed(process_binary_chunk)(
                input_file, lrecl, rec, this_count,
                column_specs, column_names, column_types, column_subtypes, column_decimals
            )
            tasks.append(task)
            rec += this_count
    return tasks

# -------------------------
# ETL for a single file
# - added param use_distributed: if True and dask.distributed is available,
#   a Client will be created inside this function (same as your original behavior).
# -------------------------
def etl_file_with_dask(input_file: str, output_file: str,
                       column_specs, column_names, column_types, column_subtypes, column_decimals, lrecl,
                       use_distributed: bool = False):
    check_and_delete_table(output_file)

    validate_specs(column_specs, column_names, lrecl)
    target_schema = build_target_schema(column_names, column_types)
    writer = pq.ParquetWriter(output_file, target_schema)

    tasks = build_tasks_for_file(input_file, column_specs, column_names, column_types, column_subtypes, column_decimals, lrecl)
    if len(tasks) == 0:
        print("No tasks to process for file:", input_file)
        writer.close()
        return

    # If asked to use distributed and it's available, create a client (as your original)
    if use_distributed and DASK_DISTRIBUTED_AVAILABLE:
        print("Starting dask.distributed Client inside file...")
        client = Client(n_workers=DASK_WORKERS, threads_per_worker=DASK_THREADS_PER_WORKER)
        futures = [client.compute(task) for task in tasks]
        try:
            for fut in dask_as_completed(futures):
                df = fut.result()
                if df is not None and getattr(df, "shape", (0,))[0] > 0:
                    write_chunk_df_with_schema(writer, df, target_schema)
        finally:
            writer.close()
            client.close()
    else:
        # local compute path (no distributed client)
        print("Computing tasks locally (per-file).")
        for task in tasks:
            df = dask.compute(task)[0]
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                write_chunk_df_with_schema(writer, df, target_schema)
        writer.close()

    print(f"Saved parquet: {output_file}")

# -------------------------
# Failed-file helpers
# -------------------------
def save_failed_file(file_path: str, base_name: str, batch_date: str):
    # append
    try:
        with open(FAILED_FILE_LOG, "a") as f:
            f.write(f"{file_path}|{base_name}|{batch_date}\n")
    except Exception as e:
        print("Failed to write to failed log:", e)

def load_failed_files():
    if not os.path.exists(FAILED_FILE_LOG):
        return []
    with open(FAILED_FILE_LOG, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    files = []
    for line in lines:
        parts = line.split("|")
        if len(parts) == 3:
            file_path, base_name, batch_date = parts
            files.append((file_path, base_name, batch_date))
    return files

# -------------------------
# Worker wrapper (this is pickled and executed in separate process)
# -------------------------
def process_file_worker(input_file: str, base_name: str, batch_date: str, var_path: str, output_folder: str, use_distributed_inside: bool = False):
    """
    This function runs in a child process.
    Returns: (success: bool, input_file, msg)
    """
    try:
        file_name = os.path.basename(input_file)
        output_file_name = file_name.rsplit(".", 1)[0]
        output_file = os.path.join(output_folder, f"{output_file_name}.parquet")

        print(f"[PID:{os.getpid()}] Processing: {input_file} -> {output_file_name}.parquet")

        column_specs, column_names, column_meta, column_types, column_subtypes, column_decimals, lrecl = \
            load_variables(var_path, base_name)

        t0 = time.time()
        etl_file_with_dask(
            input_file,
            output_file,
            column_specs,
            column_names,
            column_types,
            column_subtypes,
            column_decimals,
            lrecl,
            use_distributed=use_distributed_inside
        )
        t1 = time.time()
        return True, input_file, f"Done in {t1 - t0:.2f}s"

    except Exception as e:
        # Important: don't raise, return failure so the parent can log
        return False, input_file, str(e)

# -------------------------
# Main orchestration: submit files to ProcessPoolExecutor
# -------------------------
if __name__ == "__main__":
    start_all = time.time()

    rerun_failed_only = "--rerun-failed" in sys.argv

    if rerun_failed_only:
        print("🔄 Rerun mode: only processing failed files...")
        all_files = load_failed_files()
        if not all_files:
            print("No failed files to rerun.")
            sys.exit(0)
        # clear failed log (we'll repopulate on new failures)
        open(FAILED_FILE_LOG, "w").close()
    else:
        # fresh run: discover files
        all_files = []
        for folder in INPUT_FOLDER_PATHS:
            folder_files = get_file_names(folder)
            all_files.extend([(os.path.join(folder, f), base, batch_date) for f, base, batch_date in folder_files])
        # clear failed log at start
        open(FAILED_FILE_LOG, "w").close()

    if not all_files:
        print("No files found to process.")
        sys.exit(0)

    # Optionally, if you want a dask.distributed Client shared by all workers,
    # you would start it here and pass use_distributed_inside=True to process_file_worker.
    # But creating a single Client and sharing it across OS processes is not trivial
    # (clients are network endpoints). So by default we process files in separate processes
    # and each file will do local (non-distributed) compute.
    use_distributed_inside = False  # keep False to avoid nested distributed clients in workers

    max_workers = min(PARALLEL_FILES, len(all_files))
    print(f"Starting parallel processing of {len(all_files)} files with up to {max_workers} processes...")

    futures = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for input_file, base_name, batch_date in all_files:
            futures.append(executor.submit(process_file_worker, input_file, base_name, batch_date, VAR_PATH, OUTPUT_FOLDER_PATH, use_distributed_inside))

        # gather results as they finish
        for fut in as_completed(futures):
            try:
                success, file_path, msg = fut.result()
                if success:
                    print(f"✅ {file_path} -> {msg}")
                else:
                    print(f"❌ {file_path} failed: {msg}")
                    # parse base_name and batch_date from path if possible
                    base = os.path.basename(file_path).rsplit(".", 1)[0]
                    # try extract date suffix like name_YYYYMMDD
                    m = re.match(r"(.+?)_(\d{8})$", base)
                    if m:
                        base_name, batch_date = m.groups()
                    else:
                        base_name, batch_date = base, ""
                    save_failed_file(file_path, base_name, batch_date)
            except Exception as e:
                print("Error getting worker result:", e)

    end_all = time.time()
    print(f"\nAll done. Total time: {end_all - start_all:.2f}s")
    if os.path.exists(FAILED_FILE_LOG):
        print(f"📄 Failed file list saved at: {FAILED_FILE_LOG}")
