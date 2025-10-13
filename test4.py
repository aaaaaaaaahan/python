This Python controller is designed to monitor and manage FTP transfer files.
It performs two main functions:

Check Function – Scans the specified FTP folder to verify whether today’s dummy (empty) confirmation file has been received. The file name must include the date in the format YYYYMMDD (e.g., dummy_20251013.txt).

Delete Function – Automatically deletes any files with dates earlier than today, ensuring that only the latest FTP files are retained.

The program also maintains a daily rotating log file that records all actions taken, including files found, deleted, or skipped. This helps in tracking FTP job results and maintaining folder cleanliness.
