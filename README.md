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
```
### *.B423 Binary File Format
LEMI *.B423 binary files consist of:
1.	1024-byte ASCII header (contains Instrument number, firmware Ver, Date Time, Lat, Lon, Alt, scaling coefficients)
3.	Raw measurement data as:

| Field            | Description |
|-----------------|-------------|
| `SECOND_TIMESTAMP` | UTC timestamp (long) |
| `SAMPLE_NUM`      | Sample number (0 - FS) (short) |
| `Bx`              | Channel 1 (long) |
| `By`              | Channel 2 (long) |
| `Bz`              | Channel 3 (long) |
| `Ex`              | Channel 4 (long) |
| `Ey`              | Channel 5 (long) |
| `PPS`             | Deviation from PPS (short) |
| `PLL`             | PLL accuracy (short) |


The raw data for the above channels (Bx, By, Bz, Ex, Ey) are in integer counts.

However, additional scalings (coefficients from the header) from the *.B423 files are applied to give:
  - millivolts (mV) for the magnetic channels
  - microvolts (uV) for the electric channels

The scalings are:
  - Channel 1 = (Channel 1 * Kmx) + Ax
  - Channel 2 = (Channel 1 * Kmy) + Ay
  - Channel 3 = (Channel 1 * Kmz) + Az
  - Channel 4 = (Channel 1 * Ke1) + Ae1
  - Channel 5 = (Channel 1 * Ke2) + Ae2

Additional scaling to pyhsical units, will also have to be applied. 
  - Convert electrical data to mV/km
       - By dividing Ex and Ey by the dipole distance (m)  
  - Convert magnetic data to nT
       - Magnetic coils need calibrating from the *.RSP file
       - That have the following units:
            - Freq (Hz)
            - Magnitude (mV/nT)
            - Phase (deg)
       - Converted to the following units:
            - Freq (Hz)
            - Magnitude (mV/nT)
            - Phase (radians)

