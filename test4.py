input_file = "your_as400_file.txt"   # change this

# bytes that usually become SUB / red background
problem_bytes = {0x00, 0x1A, 0x0D, 0xFF}

with open(input_file, "rb") as f:
    data = f.read()

print("Total bytes:", len(data))
print("\n=== Affected Bytes (that will show as SUB) ===")

count = 0
for i, b in enumerate(data):
    if b in problem_bytes:
        print(f"Position {i}: Hex={hex(b)}  (Decimal={b})")
        count += 1

if count == 0:
    print("No SUB-causing bytes found.")
else:
    print(f"\nTotal affected bytes: {count}")
