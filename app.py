from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
import os
from typing import Dict, Optional, List, Tuple
from bisect import bisect_left
from wgf4_parser import get_temp_from_file, parse_header

app = FastAPI()

available_files = {} # not to check data folder each time
metadata_cache: Dict[str, Tuple] = {}  # Cache for file metadata
sorted_timestamps: List[int] = [] # To use binary search from from_ts

## Start up funcion
## Here I read all files that are in data folder
@asynccontextmanager
async def lifespan(app: FastAPI):
    global available_files, sorted_timestamps
    data_folder = "data"
    
    # Scan the `data` directory and cache file paths with timestamps
    for filename in os.listdir(data_folder):
        if filename.endswith(".wgf4"):
            try:
                timestamp = int(filename.split(".")[0])
                file_path = os.path.join(data_folder, filename)
                available_files[timestamp] = file_path
                
                # Parse header and store metadata in the cache
                metadata_cache[file_path] = await parse_header(file_path)
            except ValueError:
                continue

    # Sort the timestamps for binary search
    sorted_timestamps = sorted(available_files.keys())
    yield

app = FastAPI(lifespan=lifespan)

# Binary search to find the index of the first timestamp >= from_ts
def find_starting_index(from_ts: int) -> int:
    idx = bisect_left(sorted_timestamps, from_ts)
    return idx if idx < len(sorted_timestamps) else -1

## Main api which works with next type of requests
## curl -X GET "http://localhost:8000/getForecast?from_ts=1688400200&to_ts=1688504400&lat=52.52&lon=13.4080"
@app.get("/getForecast")
async def get_forecast(
    from_ts: int, 
    to_ts: int, 
    lat: float, 
    lon: float
) -> Dict[int, Optional[float]]:
    response = {}

    # Find the starting index for `from_ts` using binary search
    start_idx = find_starting_index(from_ts)
    
    # If no valid starting index, return an empty response
    if start_idx == -1:
        return response
    
    # Iterate over sorted timestamps within the given range
    for ts in sorted_timestamps[start_idx:]:
        if ts > to_ts:
            break
        file_path = available_files[ts]

        # Use cached metadata or parse if missing
        # We read headers only once for each file
        if file_path not in metadata_cache:
            metadata_cache[file_path] = await parse_header(file_path)

        # Fetch forecast data using cached metadata
        data = await get_temp_from_file(file_path, lat, lon, metadata_cache[file_path])
        if data is not None:
            response[ts] = data

    return response
