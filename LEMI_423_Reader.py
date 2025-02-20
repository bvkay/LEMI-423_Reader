import os
import numpy as np
import pandas as pd
import datetime

"""
LEMI-423 Data Reader
-----------------------
-  BK @ UoA, AuScope  -
-   [2025-02-20]      -
-      v0.1.1         -
-----------------------

Description:
This script reads and process LEMI-423 magnetometer binary files.
It includes:
    - Extracting metadata from the binary file headers (Read_Lemi_Header).
    - Reading and calibrating time-series data from the binary files (Read_Lemi_Data).
    - Basic error handling to the log, as logs are cool.

Usage:
    Import the module in another script or interactive environment:
        from LEMI_423_Reader import Read_Lemi_Header, Read_Lemi_Data

    To read metadata from a binary file:
        header = Read_Lemi_Header("path_to_file.B423")
        metadata = header.read()

    To read and process data from a binary file:
        data_reader = Read_Lemi_Data("path_to_file.B423", metadata["coefficients"])
        df = data_reader._from_binary()

Notes:
    - Coefficients extracted from the header are needed for calibration.

"""

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

    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(log_message)

class Read_Lemi_Header:
    """
    Reads the header of a LEMI-423 binary file and extracts metadata.

    This class:
    - Reads the first 1024 bytes of the binary file as an ASCII header.
    - Extracts site metadata including instrument number, timestamps, coordinates, and calibration coefficients.

    Args:
        binary_file (str): Path to the LEMI-423 binary file.

    Attributes:
        binary_file (str): Path to the binary file being processed.
        instrument_number (int or None): The instrument number extracted from the header.
        deployment_time (datetime or None): The deployment timestamp extracted from the header.
        latitude (float or None): Latitude coordinate of the site.
        longitude (float or None): Longitude coordinate of the site.
        elevation (float or None): Elevation of the site in meters.
        coefficients (dict): Calibration coefficients extracted from the header.

    Methods:
        read(): Reads and extracts metadata from the header.
    """

    def __init__(self, binary_file):
        """Initialize with the binary file path."""
        self.binary_file = binary_file
        self.instrument_number = None
        self.deployment_time = None
        self.latitude = self.longitude = self.elevation = None
        self.coefficients = {}

    def _read_header(self):
        """Reads and decodes the first 1024 bytes as ASCII header."""
        try:
            with open(self.binary_file, 'rb') as f:
                return f.read(1024).decode(errors='ignore').splitlines()
        except Exception as e:
            write_log(f"Error reading header from {self.binary_file}: {e}", "ERROR")
            return []

    def _extract_instrument_number(self, header):
        """Extracts instrument number from the header."""
        self.instrument_number = int(header[0].split('#')[-1]) if '#' in header[0] else None

    def _extract_deployment_time(self, header):
        """Extracts deployment date and time."""
        date, time = header[4].split()[-1], header[5].split()[-1]
        self.deployment_time = pd.to_datetime(f"{date} {time}", format="%Y/%m/%d %H:%M:%S")


    def _extract_coefficients(self, header):
        """Extracts calibration coefficients from the header."""
        self.coefficients = {
            line.split("=")[0].strip().lstrip('%'): float(line.split("=")[1])
            for line in header[13:] if "=" in line
        }

    def _extract_coordinates(self, header):
        """Extracts latitude, longitude, and elevation."""
        lat, lat_dir = header[9].split()[-1].split(',')
        lon, lon_dir = header[10].split()[-1].split(',')
        self.latitude = (int(lat[:2]) + float(lat[2:]) / 60) * (-1 if lat_dir.strip() == 'S' else 1)
        self.longitude = (int(lon[:3]) + float(lon[3:]) / 60) * (-1 if lon_dir.strip() == 'W' else 1)
        self.elevation = float(header[11].split(',')[0].split()[-1])

    def read(self):
        """
        Reads and extracts metadata from the header.

        Returns:
            dict: A dictionary containing extracted metadata including instrument number,
                  coordinates, elevation, coefficients, and deployment time.
        """
        header = self._read_header()
        if not header:
            return {}

        try:
            self._extract_instrument_number(header)
            self._extract_deployment_time(header)
            self._extract_coefficients(header)
            self._extract_coordinates(header)

            metadata = {
                "instrument_number": self.instrument_number,
                "deployment_time": self.deployment_time,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "elevation": self.elevation,
                "geo_site": f"{self.latitude:.6f}, {self.longitude:.6f}",  # Geo-reference site
                "coefficients": self.coefficients
            }

            return metadata
        
        except Exception as e:
            write_log(f"Error extracting metadata from {self.binary_file}: {e}", "ERROR")
            return {}

class Read_Lemi_Data:
    """
    Reads LEMI-423 binary data and applies calibration coefficients.

    This class:
    - Reads binary data from the LEMI-423 instrument.
    - Converts timestamps and applies calibration coefficients.
    - Stores data in a Pandas DataFrame.

    Args:
        binary_file (str): Path to the LEMI-423 binary file.
        coefficients (dict): Calibration coefficients extracted from the header.

    Attributes:
        binary_file (str): Path to the binary file being processed.
        coefficients (dict): Calibration coefficients used for data processing.
        data (pd.DataFrame or None): The processed time-series data.

    Methods:
        _from_binary(): Reads and processes the binary data.
        get_data(): Returns the processed DataFrame.
    """

    def __init__(self, binary_file, coefficients):
        """Initialize with binary file path and extracted coefficients."""
        self.binary_file = binary_file
        self.coefficients = coefficients
        self.data = None  # Will store the final DataFrame

    def _from_binary(self):
        """Reads binary data, applies calibration, and returns a Pandas DataFrame."""
        
        binary_format = np.dtype([
            ('time', 'u4'),
            ('tick', 'u2'),
            ('Bx', 'i4'),
            ('By', 'i4'),
            ('Bz', 'i4'),
            ('Ex', 'i4'),
            ('Ey', 'i4'),
            ('sync', 'b'),
            ('stage', 'B'),
            ('CRC', 'i2'),
        ])

        try:
            # Read binary file, skipping the first 1024 bytes (header)
            with open(self.binary_file, 'rb') as f:
                f.read(1024)  # Skip header
                data = np.fromfile(f, dtype=binary_format)
        
            # Convert to Pandas DataFrame
            df = pd.DataFrame(data)

            # Convert time to datetime format
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True) + pd.to_timedelta(df['tick'], unit='ms')

            # Apply calibration coefficients
            df['Bx'] = df['Bx'] * self.coefficients['Kmx'] + self.coefficients['Ax']
            df['By'] = df['By'] * self.coefficients['Kmy'] + self.coefficients['Ay']
            df['Bz'] = df['Bz'] * self.coefficients['Kmz'] + self.coefficients['Az']
            df['Ex'] = df['Ex'] * self.coefficients['Ke1'] + self.coefficients['Ae1']
            df['Ey'] = df['Ey'] * self.coefficients['Ke2'] + self.coefficients['Ae2']

            # Set the time as index
            df.set_index('time', inplace=True)

            """Returns the processed DataFrame."""
            self.data = df

            return df

        except Exception as e:
            write_log(f"Error reading binary file {self.binary_file}: {e}", "ERROR")
            return None
        
    def get_data(self):
        """
        Returns the processed DataFrame.

        Returns:
            pd.DataFrame or None: The processed time-series data.
        """
        return self.data