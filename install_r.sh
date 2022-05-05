#!/usr/bin/env bash

Rscript -e 'install.packages("lubridate")'
Rscript -e 'install.packages("stringr")'
Rscript -e 'install.packages("tidyr")'
Rscript -e 'install.packages("arules")'
Rscript -e 'install.packages("arulesSequences")'
Rscript -e 'install.packages("bupaR")'
Rscript -e 'install.packages("readr", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("vendor/bama", repos=NULL, type="source")'