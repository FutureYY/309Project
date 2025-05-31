#!/bin/bash

# exit immediately if a command exits non-zero status
set -e

# if [ -z "$CONDA_HOME" ]; then
#   echo "Please set the CONDA_HOME environment variable to your Anaconda path."
#   exit 1
# fi

# if [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
#     source "$HOME/anaconda3/etc/profile.d/conda.sh"
# elif [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
#     source "$HOME/miniconda3/etc/profile.d/conda.sh"
# else
#     echo "Could not find conda.sh. Please update run.sh with your Conda installation path."
#     exit 1
# fi

conda activate kedro-environment

echo "Setting up environment"

echo "Running Kedro pipeline"
kedro run

echo "Pipeline completed"

echo "Running data preparation script..."
python src/dataprep/prep_data.py

echo "Training model..."
python src/model/train_model.py

echo "Model saved to saved_model/"

