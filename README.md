# LEMI-423 Binary Data Processor

## Overview
This repository contains scripts for processing LEMI-423 magnetometer binary data.  
It extracts metadata from binary file headers, processes time-series data, and organizes results into structured formats.  
Supports parallel processing.

## Features
- Reads and extracts metadata from LEMI-423 binary files.
- Processes time-series data with calibration coefficients.
- Logs operations for easy debugging and monitoring.

## Requirements
- Python 3.x


## Usage

### Run the Processor
```sh
python Process_LEMI_423.py <path_to_metadata_csv> [num_cpus]
