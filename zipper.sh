#!/bin/bash

# This script will zip up the contents of the plugins directory
# and place the zip file in the root directory of the project.

output_zip="plugins.zip"
source_dir="plugins"

excluded_files=("*.pyc")
excluded_dirs=("__pycache__", "output")

zip -r "$output_zip" "$source_dir" -x "${excluded_files[@]}" "${excluded_folders[@]}"
echo "Zip process complete."