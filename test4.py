import sys

# ==========================
# CONFIG
# ==========================
input_file = "your_as400_file.txt"     # change this to your FTP file
output_clean_file = "clean_output.txt" # cleaned output
output_hex_file = "hex_dump.txt"       # hex dump output

# ==========================
# READ RAW BYTES
# ==========================
with open(input_file, "rb") as f:
    data = f.read()

print("Total bytes:", len(data))

# ==========================
# FIND BYTES THAT CAUSE SUB
# ==========================
problem_bytes = {0x00, 0x1A, 0x0D, 0xFF}  # common SUB-makers
bad_positions = []

for i, b in enumerate(data):
    if b in problem_bytes:
        bad_positions.append((i, hex(b)))

# show findings
print("\n=== Problem Bytes Found ===")
if bad_positions:
    for pos, val in bad_positions[:200]:  # limit to 200 lines
        print(f"Position {pos}: Byte {val}")
else:
    print("No obvious SUB-causing bytes detected.")

print("\nTotal problem bytes:", len(bad_positions))

# ==========================
# CREATE HEX DUMP
# ==========================
hex_dump = data.hex(" ")
with open(output_hex_file, "w") as f:
    f.write(hex_dump)

print(f"\nHex dump saved to: {output_hex_file}")

# ==========================
# TRY DECODING WITH COMMON AS400 CODEPAGES
# ==========================
def try_decode(cp):
    try:
        txt = data.decode(cp)
        print(f"\nSUCCESS decoding with {cp}")
        return txt
    except:
        print(f"\nFAILED decoding with {cp}")
        return None

decoded_037 = try_decode("cp037")
decoded_1140 = try_decode("cp1140")

# ==========================
# CLEAN THE DATA (remove SUB-causing bytes)
# ==========================
cleaned = data.replace(b"\x00", b"") \
              .replace(b"\x1A", b"") \
              .replace(b"\x0D", b"") \
              .replace(b"\xFF", b"")

try:
    clean_text = cleaned.decode("cp037", errors="replace")
except:
    clean_text = cleaned.decode("latin1", errors="replace")

with open(output_clean_file, "w", encoding="utf-8") as f:
    f.write(clean_text)

print(f"\nClean text saved to: {output_clean_file}")

# ==========================
# DONE
# ==========================
print("\n=== PROGRAM COMPLETE ===")
print("1. Look at hex_dump.txt → shows hidden bytes")
print("2. Look at clean_output.txt → cleaned version of your file")
print("3. If still wrong, upload your file and I will analyze it")
