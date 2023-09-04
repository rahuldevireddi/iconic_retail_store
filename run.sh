#!/bin/bash

INPUT_PATH="$(pwd)/iconicretail/bronze/data/raw"
OUTPUT_PATH="$(pwd)/iconicretail/gold/"
PYSPARK_SCRIPT=main.py
PACKAGE_NAME="iconicretail"

python setup.py sdist bdist_wheel

# Set the path to the generated wheel file
PACKAGE_FILE="dist/${PACKAGE_NAME}-1.0.tar.gz"

# Run spark-submit with the generated wheel file in a local mode
# Tune various commands based on cluster setup (Example: driver node memory, executor node memory..etc)
spark-submit --py-files "$PACKAGE_FILE" "$PYSPARK_SCRIPT" --input_path "$INPUT_PATH" --output_path "$OUTPUT_PATH"

#rm -rf "dist"
#rm -rf "build"
#rm -rf "${PACKAGE_NAME}.egg-info"
