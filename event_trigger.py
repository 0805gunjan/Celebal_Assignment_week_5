import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import logging

# === Setup Logging ===
log_path = "C:/Users/DELL/Desktop/trigger_log.txt"
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class TriggerHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            logging.info(f" File created: {event.src_path}")
            print(f"File created: {event.src_path}")
            try:
                logging.info(" Running export_files.py...")
                print(" Running export_files.py...")

                subprocess.run([
                    "python",
                    "C:/Users/DELL/Desktop/Celebal Projects/Celebal_Assignment_week_5/export_files.py"
                ], check=True)

                logging.info(" export_files.py completed.")
                print(" export_files.py completed.")

            except Exception as e:
                logging.error(" Failed to run export_files.py")
                logging.exception(e)
                print(" Failed to run export_files.py")

if __name__ == "__main__":
    path_to_watch = "C:/Users/DELL/Desktop/Celebal Projects/Celebal_Assignment_week_5/trigger/"

    # Make sure the trigger folder exists
    if not os.path.exists(path_to_watch):
        os.makedirs(path_to_watch)
        logging.info(f" Created trigger folder: {path_to_watch}")
        print(f"Created trigger folder: {path_to_watch}")

    # Start watching the folder
    observer = Observer()
    observer.schedule(TriggerHandler(), path=path_to_watch, recursive=False)
    observer.start()

    logging.info(f" Watching folder: {path_to_watch}")
    print(f" Watching folder: {path_to_watch}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logging.info("Stopped trigger observer.")
        print("Stopped trigger observer.")

    observer.join()

