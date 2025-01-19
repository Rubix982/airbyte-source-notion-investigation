import glob
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List
from dotenv import load_dotenv

from notion_airbyte import persist_comment_with_block_id, log_time

# Load environment variables from the .env file
load_dotenv()


def collect_ids_from_json(directory: str = "data") -> List[str]:
    """Collects IDs from JSON files in the specified directory."""
    json_files = glob.glob(os.path.join(directory, "notion-blocks-*.json"))
    ids = []

    for file_path in json_files:
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
                # Collect IDs if available, otherwise skip
                if "id" in data:
                    ids.append(data["id"])
                else:
                    print(f"No 'id' found in {file_path}")
        except (json.JSONDecodeError, FileNotFoundError) as error:
            print(f"Error reading {file_path}: {error}")

    return ids


def persist_comments_parallel(collected_ids: List[str], access_token: str, max_workers: int = 10):
    """Persists comments for a list of collected IDs using parallel execution."""

    def worker(idx: int, block_id: str):
        persist_comment_with_block_id(id=block_id, access_token=access_token, idx=idx)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda args: worker(*args), enumerate(collected_ids))


def main():
    """Main entry point for the script."""
    collected_ids = collect_ids_from_json()

    if not collected_ids:
        print("No IDs were collected. Exiting.")
        return

    # Access the environment variable
    access_token: str = os.getenv('ACCESS_TOKEN', '')
    if access_token == '':
        log_time("ACCESS_TOKEN is not set in .env. Please set the key and re-run")
        return

    persist_comments_parallel(collected_ids=collected_ids, access_token=access_token)


if __name__ == "__main__":
    main()
