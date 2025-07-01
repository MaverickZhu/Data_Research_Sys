import sys
import os
from datetime import datetime
import pandas as pd

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
from src.utils.logger import setup_logger
from tqdm import tqdm

logger = setup_logger(__name__)

def restore_collection_from_csv(collection_name: str, csv_file_path: str):
    """
    Restores a MongoDB collection from a CSV file.

    This script will:
    1. Drop the specified collection to ensure a clean slate.
    2. Read all data from the CSV file, treating all columns as strings.
    3. Insert the data into the specified collection in batches.
    
    WARNING: This is a destructive operation.
    """
    logger.warning("="*60)
    logger.warning("!!! DATABASE RESTORATION SCRIPT - USE WITH CAUTION !!!")
    logger.warning(f"This script will DROP the '{collection_name}' collection and replace its content.")
    logger.warning("="*60)

    try:
        # User confirmation
        # confirmation = input(f"Type 'YES' to confirm you want to restore '{collection_name}' from '{csv_file_path}': ")
        # if confirmation != "YES":
        #     logger.info("Restoration cancelled by user.")
        #     return

        # --- Database Connection ---
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config=config_manager.get_database_config())
        db = db_manager.get_db()

        # --- 1. Drop the existing collection ---
        logger.info(f"Dropping collection '{collection_name}'...")
        db[collection_name].drop()
        logger.info(f"Collection '{collection_name}' dropped successfully.")

        # --- 2. Read data from CSV ---
        logger.info(f"Reading data from '{csv_file_path}'...")
        # Read all columns as strings to prevent any automatic type conversion and precision loss
        df = pd.read_csv(csv_file_path, dtype=str, keep_default_na=False)
        logger.info(f"Successfully read {len(df)} records from CSV.")
        
        # Convert DataFrame to a list of dictionaries for MongoDB insertion
        records = df.to_dict('records')

        # --- 3. Insert data into MongoDB ---
        if not records:
            logger.warning("CSV file is empty. No data to insert.")
            return

        collection = db[collection_name]
        batch_size = 1000
        logger.info(f"Inserting {len(records)} records into '{collection_name}' in batches of {batch_size}...")

        inserted_count = 0
        with tqdm(total=len(records), desc=f"Restoring {collection_name}") as pbar:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                collection.insert_many(batch)
                inserted_count += len(batch)
                pbar.update(len(batch))

        logger.info("--- Restoration Complete ---")
        logger.info(f"Total records inserted: {inserted_count}")
        
        # --- 4. Verification ---
        final_count = collection.count_documents({})
        logger.info(f"Verification: Collection '{collection_name}' now contains {final_count} documents.")
        
        if inserted_count == final_count == len(df):
            logger.info("✅ Verification successful. Data has been restored correctly.")
        else:
            logger.error("❌ Verification failed. Counts do not match.")
            logger.error(f"  - Records in CSV: {len(df)}")
            logger.error(f"  - Records inserted: {inserted_count}")
            logger.error(f"  - Final count in DB: {final_count}")

    except FileNotFoundError:
        logger.error(f"Error: The file '{csv_file_path}' was not found.")
    except Exception as e:
        logger.error(f"An error occurred during the restoration process: {e}", exc_info=True)


if __name__ == "__main__":
    COLLECTION_TO_RESTORE = "xxj_shdwjbxx"
    # Assuming the backup file is in the project root directory
    CSV_BACKUP_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
        "xxj_shdwjbxx.csv"
    )
    restore_collection_from_csv(COLLECTION_TO_RESTORE, CSV_BACKUP_FILE) 