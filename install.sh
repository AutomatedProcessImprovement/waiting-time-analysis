#!/usr/bin/env bash

# Python dependencies
python3 -m venv venv
source venv/bin/activate
pip install poetry
pip install .
cd vendor/start-time-estimator; pip install -e .; cd ../..
cd vendor/batch-processing-analysis; pip install -e .; cd ../..
cd vendor/Prosimos; pip install -e .; cd ../..
poetry build

# R dependencies
Rscript -e 'install.packages("lubridate")'
Rscript -e 'install.packages("stringr")'
Rscript -e 'install.packages("tidyr")'
Rscript -e 'install.packages("arules")'
Rscript -e 'install.packages("arulesSequences")'
Rscript -e 'install.packages("bupaR")'
Rscript -e 'install.packages("readr", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("vendor/bama", repos=NULL, type="source")'