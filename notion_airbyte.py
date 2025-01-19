import base64
import concurrent.futures
import importlib
import inspect
import json
import logging
import os
import signal
from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import Union, Any, List
from dotenv import load_dotenv

import certifi
import requests
from airbyte_cdk.models import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    AirbyteMessage,
    AirbyteStream,
    SyncMode,
    Type as MessageType,
)

# Load environment variables from the .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG
)

os.makedirs("logs", exist_ok=True)

# Create logger
logger = logging.getLogger("airbyte_test")
logger.setLevel(logging.DEBUG)

# Create a FileHandler to save logs to a file
file_handler = logging.FileHandler("logs/airbyte_test.log")
file_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the file handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)


def log_time(message: str):
    logger.info(f"{message}")


def log_time_debug(message: str):
    logger.debug(f"{message}")


def log_time_warn(message: str):
    logger.warning(f"{message}")


def log_time_error(message: str):
    logger.error(f"{message}")


# Set certificate paths if needed for your environment
cert_path: str = os.getenv('CERT_PATH', '')
if cert_path != '':
    log_time_debug(f"Setting SSL_CERT_FILE to {cert_path}")
    os.environ["SSL_CERT_FILE"] = cert_path
    os.environ["REQUESTS_CA_BUNDLE"] = cert_path
    certifi.where = lambda: cert_path

# Global variable to track the last processed index
last_processed_index = 0
stop_requested = False


def save_progress(stream_name, index):
    progress_file = Path.cwd() / "logs" / f"{stream_name}-progress.json"
    with progress_file.open("w") as f:
        json.dump({"last_index": index}, f)


def load_progress(stream_name):
    progress_file = Path.cwd() / "logs" / f"{stream_name}-progress.json"
    if progress_file.exists():
        with progress_file.open("r") as f:
            return json.load(f).get("last_index", 0)
    return 0


def signal_handler(sig, frame):
    global stop_requested
    stop_requested = True
    print("Ctrl+C detected. Stopping after the current record...")


# Attach the signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)


def process_stream(stream: AirbyteStream, source: any, creds: dict):
    global last_processed_index
    global stop_requested

    if stream.name != "comments":
        log_time(f"Skipping stream: '{stream.name}'")
        return

    data_dir = Path.cwd() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Load progress from the last run
        last_processed_index = load_progress(stream.name)

        dataset: List[AirbyteMessage] = source.read(
            logger=logger,
            config=creds,
            catalog=ConfiguredAirbyteCatalog(
                streams=[
                    ConfiguredAirbyteStream(
                        stream=stream,
                        sync_mode=SyncMode.full_refresh,
                        destination_sync_mode=DestinationSyncMode.overwrite,
                    )
                ]
            ),
        )

        skipped = []

        # A function to process each record and write to a file
        def process_record(idx, data):
            if stop_requested:
                return  # Skip further processing if stop requested

            if idx < last_processed_index:
                return  # Skip already processed data

            file_name = f"notion-{stream.name}-{idx}.json"
            json_path = data_dir / file_name

            if data.record is None or len(data.record.data) == 0:
                skipped.append(json_path)
                log_time(
                    f"[STREAM: '{stream.name}', ITERATION: {idx + 1}] Skipping. Empty results."
                )
                if stream.name == "comments":
                    log_time(
                        f"[STREAM: '{stream.name}', ITERATION: {idx + 1}] Comment data: {data}"
                    )
                return

            log_time(
                f"[STREAM: '{stream.name}', ITERATION: {idx + 1}] Saving to {file_name}"
            )

            with open(json_path, "w+") as json_file_pointer:
                if data.type == MessageType.RECORD:
                    json.dump(
                        data.record.data,
                        json_file_pointer,
                        default=lambda o: f"{o} is <not serializable>",
                        indent=2,
                    )
                    json_file_pointer.write("\n")

            # Save progress after each successful processing for the dataset
            save_progress(stream.name, idx + 1)

        # Use ThreadPoolExecutor to parallelize record processing
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []

            # Submit each task to the executor
            for idx_data in enumerate(dataset):
                if stop_requested:
                    break
                future = executor.submit(process_record, *idx_data)
                futures.append(future)

            # Monitor the futures for completion or timeout
            for future in futures:
                try:
                    # Set a timeout for each thread (e.g., 60 seconds)
                    future.result(timeout=60)
                except concurrent.futures.TimeoutError:
                    log_time_warn(f"Thread timed out. Restarting...")
                    # Optionally, retry or handle the timeout error accordingly
                    future.cancel()

        if skipped:
            skipped_path = data_dir / f"notion-{stream.name}-skipped.txt"
            log_time(f"Dumping skipped files metadata to {skipped_path}")

            with open(skipped_path, "a") as fp:
                for skipped_file in skipped:
                    log_time(f"Skipped file: {str(skipped_file)}")
                    fp.write(f"{str(skipped_file)}\n")

    except Exception as e:
        log_time_error(f"Error occurred while processing stream {stream.name}: {e}")
        error_path = data_dir / f"notion-{stream.name}-error.txt"
        with open(error_path, "a") as fp:
            fp.write(f"Error: {e}\n")


def get_source(name: str, **kwargs) -> Union[Exception, Any]:
    try:
        current_directory = os.getcwd()
        log_time(f"Current Directory: {current_directory}")

        module = importlib.import_module(name)
        log_time(f"Module Imported: {module}")

        for cls_name, cls_obj in inspect.getmembers(module, inspect.isclass):
            if cls_name.startswith("Source"):
                source_instance = cls_obj(**kwargs)
                log_time(f"Instantiated Source Class: {source_instance}")

                methods = [
                    method_name
                    for method_name, method_obj in inspect.getmembers(
                        cls_obj, predicate=inspect.isfunction
                    )
                ]

                log_time(f"Methods in {cls_name}: {methods}")
                return source_instance

        return Exception("No matching class found.")

    except Exception as ex:
        log_time_error(f"Exception Occurred: {ex}")
        return ex


def sync_notion_entities():
    """Synchronizes Notion entities using the provided access token."""
    access_token: str = os.getenv('ACCESS_TOKEN', '')
    if access_token == '':
        log_time("ACCESS_TOKEN is not set in .env. Please set the key and re-run")
        return
    creds = _generate_credentials(access_token)
    _, schemas_dir = _initialize_directories()
    source = _initialize_source()
    streams = _discover_and_save_streams(source, creds, schemas_dir)
    _process_streams_concurrently(source, creds, streams)


def _generate_credentials(access_token: str) -> dict:
    """Generates credentials for Notion API."""
    return {
        "credentials": {
            "auth_type": "OAuth2.0",
            "access_token": access_token,
        }
    }


def _initialize_directories() -> tuple:
    """Creates required directories for data and schemas."""
    data_dir = _create_directory("data")
    schemas_dir = _create_directory("schemas")
    return data_dir, schemas_dir


def _create_directory(name: str) -> Path:
    """Helper to create a directory if it doesn't exist."""
    log_time(f"Creating the '{name}' directory")
    dir_path = Path.cwd() / name
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def _initialize_source():
    """Initializes the Notion data source."""
    return get_source("source_notion")


def _discover_and_save_streams(source, creds: dict, schemas_dir: Path) -> List:
    """Discovers streams and saves their schemas to a file."""
    log_time("Discovering streams...")
    source_discover = source.discover(logger=logger, config=creds)
    streams_data = _parse_stream_data(source_discover)

    schemas_path = schemas_dir / "notion.json"
    with schemas_path.open("w") as f:
        json.dump(
            streams_data,
            f,
            default=lambda obj: (
                obj.value
                if isinstance(obj, Enum)
                else TypeError(f"Type {type(obj)} not serializable")
            ),
            indent=4,
        )

    streams = source_discover.streams
    log_time(f"Discovered {len(streams)} streams")
    return streams


def _parse_stream_data(source_discover) -> dict:
    """Parses stream data from the source discovery result."""
    if callable(getattr(source_discover, "json", None)):
        return json.loads(source_discover.json())
    return asdict(source_discover)


def _process_streams_concurrently(source, creds: dict, streams: List):
    """Processes streams concurrently using a thread pool."""
    log_time("Processing streams concurrently...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_stream, s, source, creds) for s in streams]
        concurrent.futures.wait(futures)


def persist_comment_with_block_id(id, access_token, idx=0):
    """
    Change the URL and the HTTP method to test responses from the API
    """
    url = "https://api.notion.com/v1/comments"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }
    params = {
        "block_id": id
    }

    response = requests.get(url, headers=headers, params=params)
    comments_dir: str = "comments"
    file_name: str = f"response-{idx}.json"
    os.makedirs(comments_dir, exist_ok=True)
    file_name = os.path.join(comments_dir, file_name)

    results = response.json()

    if response.status_code == 200:
        if len(results.get("results", [])) == 0:
            log_time(f"Skipping saving JSON for url '{url}'. No data received. Results: {results}")
            return

        with open(file_name, "w") as f:
            json.dump(results, f, indent=4)

        log_time(f"Response saved to {file_name}")
        return

    log_time(f"Error: {response.status_code}")

    with open(file_name, "w") as f:
        json.dump(results, f, indent=4)

    log_time(f"Error response saved to {file_name}")


def persist_block_with_page_id():
    """
    Change the URL and the HTTP method to test responses from the API
    """
    page_id: str = os.getenv('PAGE_ID', '')
    if page_id == '':
        log_time("PAGE_ID is not set in .env. Please set the key and re-run")
        return

    access_token: str = os.getenv('ACCESS_TOKEN', '')
    if access_token == '':
        log_time("ACCESS_TOKEN is not set in .env. Please set the key and re-run")
        return

    url = f"https://api.notion.com/v1/blocks/{id}/children?page_size=100"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    response = requests.get(url, headers=headers)
    file_name: str = "logs/response-block.json"

    if response.status_code == 200:
        with open(file_name, "w") as f:
            json.dump(response.json(), f, indent=4)
        log_time(f"Response saved to {file_name}")
        return

    log_time(f"Error: {response.status_code}")
    with open(file_name, "w") as f:
        json.dump(response.json(), f, indent=4)
    log_time(f"Error response saved to {file_name}")


def get_access_token():
    code: str = os.getenv('CODE', '')
    if code == '':
        log_time("CODE is not set in .env. Please set the key and re-run")
        return

    client_id: str = os.getenv('CLIENT_ID', '')
    if client_id == '':
        log_time("CLIENT_ID is not set in .env. Please set the key and re-run")
        return

    client_secret: str = os.getenv('CLIENT_SECRET', '')
    if client_secret == '':
        log_time("CLIENT_SECRET is not set in .env. Please set the key and re-run")
        return

    redirect_uri: str = os.getenv('REDIRECT_URL', '')
    if redirect_uri == '':
        log_time("REDIRECT_URL is not set in .env. Please set the key and re-run")
        return

    notion_oauth_token_route: str = "https://api.notion.com/v1/oauth/token"
    encoded_credentials = base64.b64encode(
        f"{client_id}:{client_secret}".encode()
    ).decode("utf-8")
    log_time_debug(f"Encoded Credentials: {encoded_credentials}")

    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    response = requests.post(
        url=notion_oauth_token_route, headers=headers, data=json.dumps(payload)
    )

    if response.status_code != 200:
        log_time_error(f"Error response: {response.status_code}")
        log_time_error(f"Response Headers: {response.headers}")
        log_time_error(f"Response Text: {response.text}")
        raise Exception("Status received for access token is not okay")

    # Authorization URL
    # https://api.notion.com/v1/oauth/authorize?client_id=YOUR_CLIENT_ID&response_type=code&owner=user&redirect_uri=https://yourapp.com/callback
    # Sample response
    # {"message": "Response JSON: {'access_token': 'XXX_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX', 'token_type':
    # 'bearer', 'bot_id': 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX', 'workspace_name': \"XXXXXXXXXXXXXXX\", 'workspace_icon':
    # 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX', 'workspace_id':
    # 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX', 'owner': {'type': 'user', 'user': {'object': 'user', 'id':
    # 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX', 'name': 'X Y Z', 'avatar_url':
    # 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX', 'type': 'person', 'person':
    # {'email': 'XXXXXXXXXXXXXXXXXXXXXXXXXX'}}}, 'duplicated_template_id': None, 'request_id': 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'}",
    # "asctime": "2025-01-14 16:50:32,283"}}
    log_time_debug(f"Response JSON: {response.json()}")


def persist_comment_with_block_id_with_env_vars():
    id: str = os.getenv('BLOCK_ID', '')
    if id == '':
        log_time("BLOCK_ID is not set in .env. Please set the key and re-run")
        return

    access_token: str = os.getenv('ACCESS_TOKEN', '')
    if access_token == '':
        log_time("ACCESS_TOKEN is not set in .env. Please set the key and re-run")
        return

    persist_comment_with_block_id(id=id, access_token=access_token)


def main():
    # get_access_token()
    # persist_comment_with_block_id_with_env_vars()
    # persist_block_with_page_id()
    sync_notion_entities()


if __name__ == "__main__":
    main()
