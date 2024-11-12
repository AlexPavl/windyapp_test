import asyncio
import aiofiles
from array import array
from typing import Optional, Tuple

# Constants to interpret the file header
# Header from example
# $minY = $ar[1];
# $maxY = $ar[2];
# $minX = $ar[3];
# $maxX = $ar[4];
# $dy = $ar[5];
# $dx = $ar[6];
# $multiplier = $ar[7];
# $emptyValue = $ar['empty_value'];
HEADER_SIZE = 8 * 4  # 8 integers, each 4 bytes

# Semaphore to control concurrent file access
file_read_semaphore = asyncio.Semaphore(10)

# Parses the file header to get metadata about grid boundaries and resolution
async def parse_header(file_path: str) -> Tuple[int, int, int, int, int, int, int, float]:
    async with file_read_semaphore:
        async with aiofiles.open(file_path, "rb") as f:
            # Read the header: 7 integers + 1 float (for empty_value)
            header_data = await f.read(HEADER_SIZE)
            header = array("i", header_data[:28])  # First 7 integers as header
            empty_value_data = header_data[28:]
            empty_value = array("f", empty_value_data)[0]
            
            latitude1 = header[0]
            latitude2 = header[1]
            longitude1 = header[2]
            longitude2 = header[3]
            dy = header[4]
            dx = header[5]
            multiplier = header[6]
            
            return (latitude1, latitude2, longitude1, longitude2, dy, dx, multiplier, empty_value)

# Reads the binary weather data from the file and returns the value at given coordinates
async def get_temp_from_file(
    file_path: str,
    lat: float,
    lon: float,
    metadata: Tuple[int, int, int, int, int, int, int, float]
) -> Optional[float]:
    # Unpack metadata values
    lat_bottom, lat_top, lon_left, lon_right, dy, dx, multiplier, empty_value = metadata

    # Convert latitude and longitude to integer format used in the file
    target_lat = int(lat * multiplier)
    target_lon = int(lon * multiplier)

    # Are coordinates are within the grid boundaries
    if not (lat_bottom <= target_lat <= lat_top and lon_left <= target_lon <= lon_right):
        return None
    
    # Calculate grid indices based on the latitude/longitude steps
    lat_index = (target_lat - lat_bottom) // dy
    lon_index = (target_lon - lon_left) // dx
    grid_width = (lon_right - lon_left) // dx + 1

    # Calculate the flat index in the 1D data array
    data_index = lat_index * grid_width + lon_index

    async with file_read_semaphore:
        async with aiofiles.open(file_path, "rb") as f:
            # Read the specific value we need by seeking to the correct index
            await f.seek(data_index * 4 + HEADER_SIZE + 4, 1)  # Each float is 4 bytes, HEADER_SIZE + 4 bytes for the float empty_value marker
            data_value_bytes = await f.read(4)
            
            if not data_value_bytes:
                return None  # End of file or invalid data location
            
            # Convert the binary data to a float
            data_value = array("f", data_value_bytes)[0]
            
            # If the value matches the empty value marker, treat it as None
            if data_value == empty_value:
                return None

            return data_value
