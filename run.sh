#!/bin/bash

# Exit on first error
set -e

echo "====================================="
echo "🚀 Starting Kedro pipeline execution..."
echo "====================================="
kedro run

echo "====================================="
echo "Splitting data into train/test..."
echo "====================================="
python src/pipeline/split_data.py

echo "====================================="
echo "Training the model..."
echo "====================================="
python src/model/train_model.py

echo "====================================="
echo "Evaluating the model..."
echo "====================================="
python src/model/evaluate_model.py

echo "All steps completed successfully!"

