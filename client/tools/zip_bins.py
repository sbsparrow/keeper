#!/usr/bin/env python3
import argparse
import os
from zipfile import ZipFile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", help="The output zip file", required=True)
    parser.add_argument("files", nargs="+", help="List of files to zip")
    args = parser.parse_args()
    output = os.path.normpath(args.output)
    files = [os.path.normpath(file) for file in args.files]

    try:
        with ZipFile(output, "w") as zf:
            for file in files:
                try:
                    zf.write(file, arcname=os.path.basename(file))
                except FileNotFoundError:
                    print(f"Skipping {file}. Not found on disk.")
    except FileNotFoundError:
        print(f"Can't create {output}. Parent directory does not exist.")

if __name__ == "__main__":
    main()
