FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y
RUN apt-get -y install tzdata
RUN ln -fs /usr/share/zoneinfo/Europe/Tallinn /etc/localtime
RUN apt-get install -y r-base # installs R v3.6.3

RUN Rscript -e 'install.packages("lubridate")'
RUN Rscript -e 'install.packages("stringr")'
RUN Rscript -e 'install.packages("tidyr")'
RUN Rscript -e 'install.packages("arules")'
RUN Rscript -e 'install.packages("arulesSequences")'
RUN Rscript -e 'install.packages("bupaR")'
RUN Rscript -e 'install.packages("readr", repos="http://cran.us.r-project.org")'

CMD ["bash"]