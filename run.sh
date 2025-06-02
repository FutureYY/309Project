#!/bin/bash

# Exit on first error
set -e

# Set environment name
ENV_NAME=".venv"

echo "Checking if virtual environment exists..."
if [ ! -d "$ENV_NAME" ]; then
  echo "Creating virtual environment..."
  python -m venv $ENV_NAME
fi

echo "Activating virtual environment..."
source $ENV_NAME/bin/activate

echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Running Kedro pipeline..."
kedro run

echo "Training model..."
python src/model/train_model.py

echo "Done. Model saved to saved_model/"

echo "Starting Flask app..."
python app.py

