input_file = "your_as400_file.txt"     # your FTP file
output_file = "clean_file.txt"         # cleaned text output

# bytes that usually cause SUB
problem_bytes = {0x00, 0x1A, 0x0D, 0xFF}

# read file as raw bytes
with open(input_file, "rb") as f:
    data = f.read()

# remove all unwanted bytes
cleaned = bytes(b for b in data if b not in problem_bytes)

# decode to text (best effort)
clean_text = cleaned.decode("cp037", errors="ignore")

# save final result
with open(output_file, "w", encoding="utf-8") as f:
    f.write(clean_text)

print("Done. Clean file saved as:", output_file)
