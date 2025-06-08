#!/bin/bash

# Exit on first error
set -e

echo "====================================="
echo "Starting Kedro pipeline execution..."
echo "====================================="
kedro run --pipeline "dataprep"

echo "====================================="
echo "Training the model..."
echo "====================================="
kedro run --pipeline "model"

echo "====================================="
echo "Evaluating the model..."
echo "====================================="
kedro run --pipeline "evaluation"

echo "All steps completed successfully!"
