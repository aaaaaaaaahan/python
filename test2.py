import os
import datetime
import glob

# Base name like GDG
base_name = "detail_report"

# Add timestamp (yyyymmdd)
today = datetime.date.today().strftime("%Y%m%d")
detail_txt = f"{base_name}_{today}.txt"

# Write your report
with open(detail_txt, "w") as f:
    f.write("This is today's report\n")

print("Report written:", detail_txt)

# Keep only last 3 generations (like GDG LIMIT)
max_versions = 3
files = sorted(glob.glob(f"{base_name}_*.txt"))
if len(files) > max_versions:
    for old_file in files[:-max_versions]:
        os.remove(old_file)
        print("Deleted old generation:", old_file)
