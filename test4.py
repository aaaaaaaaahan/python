import os

input_file = "input_from_ftp.txt"  # Change this
output_file = "cleaned_output"     # Extension added automatically

# List of bytes that cause SUB/unreadable symbols
BAD_BYTES = set(range(0x00, 0x20)) | {0x0D, 0x1A, 0x25, 0xFF} | {0x4A, 0x4F, 0x5A, 0x5F, 0x6A, 0x6E} | {0x85, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96}

# -------------------------------------------------------------
# READ FILE
# -------------------------------------------------------------
with open(input_file, "rb") as f:
    data = f.read()

# -------------------------------------------------------------
# DETECT TYPE: binary or text (heuristic)
# -------------------------------------------------------------
# If >5% of bytes are non-printable, treat as binary
non_printable = sum(1 for b in data if b < 0x20 or b > 0x7E)
if non_printable / len(data) > 0.05:
    file_type = "binary"
else:
    file_type = "text"

print(f"Detected file type: {file_type}")

# -------------------------------------------------------------
# CLEAN FILE
# -------------------------------------------------------------
if file_type == "text":
    # Remove bad bytes and decode from EBCDIC to UTF-8
    cleaned_bytes = bytes(b if b not in BAD_BYTES else 0x40 for b in data)  # EBCDIC space
    try:
        cleaned_text = cleaned_bytes.decode("cp1140")
    except:
        cleaned_text = cleaned_bytes.decode("cp037")
    
    output_file += ".txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(cleaned_text)

else:  # binary
    # Remove bad bytes, keep as bytes
    cleaned_bytes = bytes(b for b in data if b not in BAD_BYTES)
    output_file += ".bin"
    with open(output_file, "wb") as f:
        f.write(cleaned_bytes)

print(f"Done. Clean file saved as: {output_file}")
