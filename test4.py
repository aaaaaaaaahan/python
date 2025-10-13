import os
import re
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

# =========================================
# Configuration
# =========================================
FTP_FOLDER = "/host/ftp/incoming"
LOG_FILE = "/host/ftp/ftp_controller.log"

# =========================================
# Setup Logging with Daily Rotation
# =========================================
logger = logging.getLogger("ftp_controller")
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=7)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# =========================================
# Function 1: Check today's dummy file
# =========================================
def check_dummy_file():
    today = datetime.date.today()
    found_today = False

    if not os.path.exists(FTP_FOLDER):
        logger.warning(f"FTP folder not found: {FTP_FOLDER}")
        return False

    for file in os.listdir(FTP_FOLDER):
        file_path = os.path.join(FTP_FOLDER, file)
        if os.path.isfile(file_path):
            # Extract date from filename pattern like dummy_YYYYMMDD.txt
            match = re.search(r'(\d{8})', file)
            if match:
                file_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()
                if file_date == today:
                    found_today = True
                    logger.info(f"Found today's dummy file: {file}")
            else:
                logger.info(f"Skipped file (no date in name): {file}")

    if not found_today:
        logger.warning("No dummy file found for today.")
    return found_today


# =========================================
# Function 2: Delete old files
# =========================================
def delete_old_files():
    today = datetime.date.today()

    if not os.path.exists(FTP_FOLDER):
        logger.warning(f"FTP folder not found: {FTP_FOLDER}")
        return

    for file in os.listdir(FTP_FOLDER):
        file_path = os.path.join(FTP_FOLDER, file)
        if os.path.isfile(file_path):
            match = re.search(r'(\d{8})', file)
            if match:
                file_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()
                if file_date < today:
                    try:
                        os.remove(file_path)
                        logger.info(f"Deleted old file: {file} (file date: {file_date})")
                    except Exception as e:
                        logger.error(f"Error deleting {file}: {e}")
                else:
                    logger.info(f"Keep file (today or future): {file}")
            else:
                logger.info(f"Skipped file (no date in name): {file}")


# =========================================
# Main Controller Flow
# =========================================
if __name__ == "__main__":
    logger.info("===== FTP Controller Started =====")

    # Step 1: Check today's dummy file
    if check_dummy_file():
        logger.info("Today's dummy file found ✅")
    else:
        logger.info("Today's dummy file not found ❌")

    # Step 2: Delete old files
    delete_old_files()

    logger.info("===== FTP Controller Completed =====")
