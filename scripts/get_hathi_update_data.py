#!/usr/bin/env python3
"""
Download a local copy of HathiTrust update data for analysis.
"""

import json
import pathlib

import requests
from tqdm import tqdm

DATA_DIR = pathlib.Path(__file__).parent.parent.resolve() / "data" / "hathi" / "updates"

FILE_LIST_URL = "https://www.hathitrust.org/files/hathifiles/hathi_file_list.json"


def download_file(url:str, filename: pathlib.Path, expected_size: int | None = None) -> pathlib.Path:
    if filename.exists():
        # check size if known
        if expected_size is None or filename.stat().st_size == expected_size:
            print(f"File {filename.name} already exists; skipping download")
            return filename
        else:
            print(f"File {filename.name} exists but does not have expected size; redownloading")
            filename.unlink()

    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        # wrap content iterator with tqdm to report status for large files
        # adapted from https://stackoverflow.com/a/75335430
        progress_chunks = tqdm(
            response.iter_content(chunk_size=8192),
            total=expected_size,  # in bytes
            desc=f"Downloading {filename.name}",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,  # make use of standard units e.g. KB, MB, etc.
            miniters=1,  # recommended for network progress that might vary strongly
        )
        with filename.open('wb') as filehandle:
            for chunk in progress_chunks:
                filehandle.write(chunk)

    return filename

def main():
    print(f"Downloading HathiTrust update data. All files will be saved in {DATA_DIR}")

    file_list_filename = download_file(FILE_LIST_URL, DATA_DIR / "hathi_file_list.json")
    file_list = json.load(file_list_filename.open())
    for file in file_list:
        download_file(file["url"], DATA_DIR / file["filename"], file['size'])

if __name__ == "__main__":
    main()
