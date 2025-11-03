import os

for table, filename in outputs.items():
    write_fixed_width_txt(table, os.path.join(csv_output_path, filename), title=table)
