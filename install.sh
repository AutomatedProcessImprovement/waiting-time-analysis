#!/usr/bin/env bash

python3 -m venv venv
source venv/bin/activate
pip install poetry
poetry install

cd vendor/start-time-estimator; pip install -e .; cd ../..
cd vendor/batch-processing-analysis; pip install -e .; cd ../..
cd vendor/Prosimos; pip install -e .; cd ../..
