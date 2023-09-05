#!/bin/bash

# This script will zip up the contents of the plugins directory
# and place the zip file in the root directory of the project.

output_zip="plugins.zip"
source_dir="plugins"

zip -r $output_zip $(find $source_dir -type d -name "__pycache__" -prune -o -print)
echo "Zip process complete."