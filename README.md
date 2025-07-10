# CHR2025 unstable data paper

Code, data, and LaTex for CHR2025 short paper.

## Installation

Python dependencies are specified in `pyproject.toml`; use `pip`, `uv pip`, or similar to install them:

```bash
pip install .
```

Developed with Python 3.12.

## HathiTrust update data

HathiTrust update data used for analysis were downloaded from:
https://www.hathitrust.org/member-libraries/resources-for-librarians/data-resources/hathifiles/

Due to the size of these files, they were not included in this repository.

To download a copy, use the script provided: `python ./scripts/get_hathi_update_data.py`

Files will be saved in `data/hath/updates/`.

A copy of the hathi file list at the time of writing is included at `data/hathi/_hathi_file_list.json`.
