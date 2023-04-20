from google.cloud import storage
import os

# Parse Parquet records into JSON format
def parse_parquet(element):
    data = element.to_dict()
    return data

# Function to get the list of new files
def get_new_files(project_id, bucket_name, input, processed_files_path):
    # Set up GCP storage client
    client = storage.Client(project = project_id)
    bucket = client.get_bucket(bucket_name)

    # Get all parquet files in the folder
    blobs = bucket.list_blobs(prefix=input)

    # Get the list of processed files
    processed_files = set()
    if os.path.exists(processed_files_path):
        with open(processed_files_path, "r") as f:
            processed_files = set(line.strip() for line in f)

    # Get new files that haven't been processed yet
    new_files = [blob.name for blob in blobs if blob.name not in processed_files]

    return new_files


# Update the list of processed files
def update_processed_files(file_name, processed_files_path):
    with open(processed_files_path, "a") as f:
        f.write(file_name + "\n")
