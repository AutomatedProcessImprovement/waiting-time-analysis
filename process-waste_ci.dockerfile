FROM nokal/process-waste-rlang_v3.6.3:latest

WORKDIR /usr/src/app
ADD . /usr/src/app

RUN Rscript -e 'install.packages("vendor/bama", repos=NULL, type="source")'
RUN export RSCRIPT_BIN_PATH=`which Rscript`
RUN bash install_python.sh

CMD ["bash"]
