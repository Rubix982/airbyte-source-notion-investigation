import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from read_block_ids import collect_ids_from_json


def extract_block_ids(file_path):
    # Define the regex pattern to match the BlockId
    pattern = re.compile(r'HTTPRequesterComments: BlockId: ([a-f0-9\-]+)')
    block_ids = []

    # Open the file and read it line by line
    with open(file_path, 'r') as file:
        for line in file:
            match = pattern.search(line.strip())
            if match:
                block_ids.append(match.group(1))  # Extract the BlockId value

    return block_ids


def compare_lists(list1, list2):
    # Convert lists to sets to remove duplicates and perform set operations
    set1 = set(list1)
    set2 = set(list2)

    # Find elements in list1 that are not in list2
    list1_uniques = set1 - set2

    # Find elements in list2 that are not in list1
    list2_uniques = set2 - set1

    return list1_uniques, list2_uniques


def grep_id(id):
    path = Path.home() / "code" / "notion" / "data"

    # Run the grep command and capture the output
    try:
        result = subprocess.run(
            ['grep', '-r', id, str(path)],  # Search recursively for the ID
            capture_output=True,
            text=True
        )
        return id, result.stdout.strip()  # Return ID and the output from grep
    except subprocess.CalledProcessError as e:
        return id, f"Error during grep: {e}"


def main():
    ids_from_airbyte_logs = extract_block_ids("logs/comments_only.txt")
    ids_from_block_jsons = collect_ids_from_json()
    unique_airbyte_log_ids, unique_block_json_ids = compare_lists(ids_from_airbyte_logs, ids_from_block_jsons)

    # Using ThreadPoolExecutor to parallelize the grep process for each ID
    with ThreadPoolExecutor() as executor:
        # Submit grep tasks for each ID, collect the results
        airbyte_log_results = executor.map(grep_id, unique_airbyte_log_ids)
        block_json_results = executor.map(grep_id, unique_block_json_ids)

    # Convert results to dictionaries
    json_output = {
        "unique_airbyte_log_ids": {
            "ids": list(unique_airbyte_log_ids),
            "search_results": {id: result for id, result in airbyte_log_results}
        },
        "unique_block_json_ids": {
            "ids": list(unique_block_json_ids),
            "search_results": {id: result for id, result in block_json_results}
        }
    }

    with open("logs/grep_results.json", "w+") as fp:
        json.dump(json_output, fp, indent=4)


if __name__ == "__main__":
    main()
