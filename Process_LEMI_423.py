import os
import datetime
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from LEMI_423_Reader import Read_Lemi_Header, Read_Lemi_Data

"""
LEMI-423 Processor
-----------------------
-  BK @ UoA, AuScope  -
-   [2025-02-20]      -
-      v0.1.9         -
-----------------------

Description:
This script processes LEMI-423 magnetometer binary files.
Does it by loading the survey metadata csv file and extracting the site names and other metadata.
Every sites binary data, must be structured in their own folders, named after the site name.
As the script uses that to search for the folders and data to load.
Multiprocessing supported.
Keeps track of whats happening, as logs are cool.

Usage:
    python Process_LEMI_423.py <path_to_metadata_csv>

    Optionally, specify the number of CPU cores to use for multiprocessing:
        python Process_LEMI_423.py <path_to_metadata_csv> <num_cpus>

    If no CPU count is provided, the script defaults to using up to 4 cores or the system's available count (whichever is lower).

Notes:
    - Ensure that the metadata csv file contains all required columns.
    - Binary files must be stored in folders named after there site name.

"""

# Define log file path
LOG_FILE = "log.txt"

def write_log(message, level="INFO"):
    """
    Writes log messages to log.txt.

    Args:
        message (str): The log message to be written.
        level (str, optional): Log level (INFO, WARNING, ERROR). Defaults to "INFO".

    Returns:
        None
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"{timestamp} - {level} - {message}\n"

    # Append log message to file
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(log_message)

# Start log
write_log("===== LEMI-423 Processing Started =====")

def load_metadata(csv_file):
    """
    Loads metadata from the provided CSV file.

    Args:
        csv_file (str): Path to the metadata CSV file.

    Returns:
        pd.DataFrame: DataFrame containing metadata for all sites.
    """
    required_columns = ["SiteName", "ExDipole", "ExAzimuth", "EyDipole", "EyAzimuth"]
    
    try:
        df = pd.read_csv(csv_file)

        # Ensure all required columns exist
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in metadata CSV: {', '.join(missing_cols)}")

        write_log(f"Successfully loaded metadata from {csv_file}")        
        return df[required_columns].dropna()
    except Exception as e:
        write_log(f"Error loading metadata CSV: {e}", "ERROR")
        return None

def process_site(directory, site_info, site_id):
    """
    Processes LEMI-423 binary files for a given site.
    Logs which process (CPU) handles each site.

    Args:
        directory (str): Path to the base directory containing site folders.
        site_info (pd.Series): Metadata row containing site details from the CSV file.
        site_id (int): Unique ID assigned to each site.

    Returns:
        tuple: 
            - pd.DataFrame: Time-series data for the site.
            - dict: Site metadata including start/finish times and sample rate.
    """
    process_id = os.getpid()  # Get current process ID
    site_name = site_info["SiteName"]
    site_path = os.path.join(directory, site_name)

    write_log(f"Process {process_id} handling site: {site_name}")

    if not os.path.isdir(site_path):
        write_log(f"Site folder '{site_name}' not found.", "WARNING")
        return None, None

    # Sort binary files in ascending order based on Unix timestamp in the filename
    binary_files = sorted(
        [os.path.join(site_path, f) for f in os.listdir(site_path) if f.endswith(".B423")],
        key=lambda x: int(os.path.basename(x).split('.')[0])
    )

    if not binary_files:
        write_log(f" No *.B423 files found in '{site_name}'.", "WARNING")
        return None, None

    all_data = []
    start_time = None
    finish_time = None
    sample_rate = None

    for binary_file in binary_files:
        write_log(f"Process {process_id} processing file: {binary_file}")

        try:
            # Read and extract metadata from the binary file header
            header_reader = Read_Lemi_Header(binary_file)
            header_info = header_reader.read()

            # Load and process binary data
            data_reader = Read_Lemi_Data(binary_file, header_info["coefficients"])
            df = data_reader._from_binary()

            # Extract start and finish time from the first and last timestamps
            start_time = start_time or df.index.min()
            finish_time = df.index.max()

            # Calculate sample rate based on the number of samples in one second
            if sample_rate is None:
                sample_rate = len(df[df.index < df.index.min() + pd.Timedelta(seconds=1)])

            # Remove unnecessary columns to save memory
            df.drop(columns=["time", "Bz", "tick", "sync", "stage", "CRC"], errors='ignore', inplace=True)

            # Add site ID for easier merging later
            df["site_id"] = site_id
            all_data.append(df)

        except Exception as e:
            write_log(f"Process {process_id} error processing {binary_file}: {e}", "ERROR")
            continue

    # Store metadata separately for each site
    metadata = {
        "site_id": site_id,
        "site_name": site_name,
        "Rx_no.": header_info["instrument_number"],
        "Lat": f"{header_info['latitude']:.6f}",
        "Lon": f"{header_info['longitude']:.6f}",
        "Alt": header_info["elevation"],
        "Start Time": start_time,
        "Finish Time": finish_time,
        "Sample Rate": sample_rate,
        "ExDipole": site_info["ExDipole"],
        "ExAz": site_info["ExAzimuth"],
        "EyDipole": site_info["EyDipole"],
        "EyAz": site_info["EyAzimuth"]
    }

    write_log(f"Process {process_id} finished site: {site_name}")
    return pd.concat(all_data) if all_data else None, metadata

def process_all_sites(csv_file, num_workers = None):
    """
    Processes all sites using multiprocessing.

    Args:
        csv_file (str): Path to the metadata CSV file.
        num_workers (int, optional): Number of CPU cores to use. Defaults to 4.

    Returns:
        tuple: 
            - pd.DataFrame: Merged time-series data from all sites.
            - pd.DataFrame: Metadata table for all processed sites.
    """
    directory = os.path.dirname(csv_file)
    metadata_df = load_metadata(csv_file)

    if metadata_df is None:
        return None, None

    all_data = []
    metadata_list = []

    # If user didn't specify, set default to min (4, available CPUs)
    if num_workers is None:
        num_workers = min(4, os.cpu_count() or 1)
    else:
        num_workers = max(1, min(num_workers, os.cpu_count() or 1))  # Ensure valid range


    # Process sites in parallel using multiprocessing
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_site, directory, row, i): i for i, row in metadata_df.iterrows()}
        for future in futures:
            try:
                df, metadata = future.result()
                if df is not None:
                    all_data.append(df)
                    metadata_list.append(metadata)
            except Exception as e:
                write_log(f"Error in multiprocessing: {e}", "ERROR")

    return pd.concat(all_data) if all_data else None, pd.DataFrame(metadata_list)

def log_summary(time_series_df, metadata_df):
    """
    Logs a summary of the processed dataset.

    Args:
        time_series_df (pd.DataFrame): The merged time-series dataset.
        metadata_df (pd.DataFrame): The metadata table.

    Returns:
        None
    """
    if time_series_df is None or time_series_df.empty:
        write_log("No data loaded.", "WARNING")
        return

    write_log("\n **Site Metadata**", "INFO")

    # Save metadata table to log file
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(metadata_df.to_string() + "\n\n")

    # Log first 5 rows per site for QA
    write_log("\n **First 5 rows of data per site for QA:**", "INFO")
    for site_id in metadata_df["site_id"].unique():
        site_name = metadata_df.loc[metadata_df["site_id"] == site_id, "site_name"].values[0]
        site_df = time_series_df[time_series_df["site_id"] == site_id]

        write_log(f"\n Site: {site_name} (site_id: {site_id})", "INFO")
        with open(LOG_FILE, "a", encoding="utf-8") as log_file:
            log_file.write(site_df.head(5).to_string() + "\n" + "-" * 50 + "\n")

if __name__ == "__main__":
    csv_file = input("\n  Enter the metadata CSV file path: ").strip()
    write_log(f"CSV file: {csv_file}")

    try:
        num_workers = int(input("  Enter the number of CPU cores to use (default: 4): ").strip())
    except ValueError:
        num_workers = None  # Use default if input is invalid
    write_log(f"Number CPUs: {num_workers}")

    if os.path.isfile(csv_file):
        time_series_data, site_metadata = process_all_sites(csv_file, num_workers)

        if time_series_data is not None and site_metadata is not None:
            write_log("All sites processed successfully!")
            log_summary(time_series_data, site_metadata)
    else:
        write_log("Invalid CSV file path.", "ERROR")