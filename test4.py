input_file = "your_as400_file.txt"
output_file = "clean_file.txt"

# =========================
# BAD BYTES (complete list)
# =========================
BAD_BYTES = (
    set(range(0x00, 0x20))       # control chars 00–1F
    | {0x0D, 0x1A, 0x25, 0xFF}   # common SUB / CR / invalid
    | {0x4A, 0x4F, 0x5A, 0x5F, 0x6A, 0x6E}  # EBCDIC weird chars
    | {0x85, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96}  # SAS special chars
)

# read raw bytes
with open(input_file, "rb") as f:
    data = f.read()

# replace bad bytes with space (0x40 in EBCDIC, 0x20 in ASCII)
cleaned = bytearray()
for b in data:
    if b in BAD_BYTES:
        cleaned.append(0x40)    # EBCDIC space (safer)
    else:
        cleaned.append(b)

# decode EBCDIC → UTF-8 correctly
try:
    text = cleaned.decode("cp1140")
except:
    text = cleaned.decode("cp037")

# save clean readable file
with open(output_file, "w", encoding="utf-8") as f:
    f.write(text)

print("Done. Clean file saved:", output_file)
