#!/bin/bash

# Exit on first error
set -e

echo "Running Kedro pipeline..."
kedro run

echo "Training model..."
python src/model/train_model.py

echo "Model training is completed, saving model "

while true; do
    echo "Select an option:"
    echo "1) Show prediction result in terminal"
    echo "2) Run Flask app in Docker"
    read -p "Enter choice [1 or 2]: " choice

    if [ "$choice" == "1" ]; then
        echo "Running prediction script..."
        python app.py
        break

    elif [ "$choice" == "2" ]; then
        echo "Building Docker image..."
        docker build -t my-flask-app .

        echo "Running Docker container..."
        docker run -p 5000:5000 my-flask-app
        break

    else
        echo "Invalid choice. Please try again."
    fi
done


