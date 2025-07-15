#!/usr/bin/env python3
#
import argparse
import csv
import datetime
import pathlib
import os

def main(basedir, ext, output):
    extensions = ext.split(',')
    with output.open('w') as csvfile:
        fieldnames = ['filename', 'mtime', 'last_modified']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        # use os.walk so this can run under python 3.11
        for (dir, _dirnames, filenames) in os.walk(basedir):
            dir_path = pathlib.Path(dir)
            for file in filenames:
                file_path = dir_path / file
                if file_path.is_file() and file_path.suffix.lstrip('.') in extensions:
                    mod_time = file_path.stat().st_mtime
                    # include the full filename, because we need the prefix to convert filename to HTID
                    writer.writerow({
                        'filename': str(file_path.relative_to(basedir)),
                        'mtime': mod_time,
                        'last_modified': datetime.datetime.fromtimestamp(mod_time)
                    })


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Crawl a directory and generate a CSV of files and last modification time')
    parser.add_argument('dir', help='Directory to search', type=pathlib.Path)
    parser.add_argument('ext', help='File extensions to match')
    parser.add_argument("-o", "--output", help="Output file", type=pathlib.Path)
    args = parser.parse_args()

    main(args.dir, args.ext, args.output)
