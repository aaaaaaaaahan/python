import os
import datetime
import logging

# =========================================
# Configuration
# =========================================
FTP_FOLDER = "/host/ftp/incoming"
LOG_FILE = "/host/ftp/ftp_controller.log"

# =========================================
# Setup Logging
# =========================================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================================
# Function 1: Check for today's dummy file
# =========================================
def check_dummy_file():
    today = datetime.date.today()
    found_today = False

    if not os.path.exists(FTP_FOLDER):
        logging.warning(f"FTP folder not found: {FTP_FOLDER}")
        return False

    for file in os.listdir(FTP_FOLDER):
        file_path = os.path.join(FTP_FOLDER, file)
        if os.path.isfile(file_path):
            modified_date = datetime.date.fromtimestamp(os.path.getmtime(file_path))
            # check file date is today
            if modified_date == today:
                found_today = True
                logging.info(f"Found today's file: {file}")
    
    if not found_today:
        logging.warning("No dummy file found for today.")
    return found_today


# =========================================
# Function 2: Delete old files
# =========================================
def delete_old_files():
    today = datetime.date.today()

    if not os.path.exists(FTP_FOLDER):
        logging.warning(f"FTP folder not found: {FTP_FOLDER}")
        return

    for file in os.listdir(FTP_FOLDER):
        file_path = os.path.join(FTP_FOLDER, file)
        if os.path.isfile(file_path):
            modified_date = datetime.date.fromtimestamp(os.path.getmtime(file_path))
            if modified_date < today:
                try:
                    os.remove(file_path)
                    logging.info(f"Deleted old file: {file} (modified: {modified_date})")
                except Exception as e:
                    logging.error(f"Error deleting {file}: {e}")

# =========================================
# Main Controller Flow
# =========================================
if __name__ == "__main__":
    logging.info("===== FTP Controller Started =====")

    # Step 1: Check today's dummy file
    if check_dummy_file():
        logging.info("Today's dummy file found ✅")
    else:
        logging.info("Today's dummy file not found ❌")

    # Step 2: Delete old files
    delete_old_files()

    logging.info("===== FTP Controller Completed =====")
