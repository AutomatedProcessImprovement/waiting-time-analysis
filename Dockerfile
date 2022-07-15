FROM nokal/process-waste-rlang_v3.6.3:latest

RUN apt-get -y update && apt-get -y install  \
    python3.10 \
    python3-pip \
    python3.10-venv \
    git

WORKDIR /usr/src/app
ADD . /usr/src/app

RUN Rscript -e 'install.packages("vendor/bama", repos=NULL, type="source")'
RUN echo RSCRIPT_BIN_PATH=`which Rscript` >> /etc/environment
RUN bash install_python.sh

CMD ["bash"]
