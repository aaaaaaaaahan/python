import os
import re
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

root_paths = [
    "/host/cis/input/sas_dataset",
	"/host/cis/parquet/sas_parquet",
]

log_file = "/host/cis/input/housekeeping.log"
logger = logging.getLogger("housekeeping")
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
handler.suffix = "%Y%m%d"
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

pattern = re.compile(r".*_(\d{8})(?:\..*)?$") 

cutoff_date = datetime.date.today() - datetime.timedelta(days=2)

for root_path in root_paths:
	logger.info(f"Starting housekeeping in {root_path}, cutoff date = {cutoff_date}")

	deleted_files = 0
	skipped_files = 0

	for dirpath, _, filenames in os.walk(root_path):
		for filename in filenames:
			match = pattern.match(filename)
			if not match:
				skipped_files += 1
				continue

			try:
				file_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()
			except ValueError:
				skipped_files += 1
				continue

			if file_date < cutoff_date:
				file_path = os.path.join(dirpath,filename)
				try:
					os.remove(file_path)
					logger.info(f"Deleted: {file_path} (date={file_date})")
					deleted_files += 1
				except Exception as e:	
					logger.error(f"Failed to delete {file_path}: {e}")

			else:
				skipped_files += 1

logger.info(f"Finished {root_path}. Deleted {deleted_files} files, skipped {skipped_files}.")
