from pathlib import Path

output_folder = Path(csv_output_path())  # note the parentheses!
output_folder.mkdir(parents=True, exist_ok=True)

for table, filename in outputs.items():
    write_fixed_width_txt(table, output_folder / filename, title=table)
