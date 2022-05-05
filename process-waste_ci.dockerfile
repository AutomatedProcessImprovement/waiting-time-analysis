FROM nokal/process-waste-rlang_v3.6.3:latest

WORKDIR /usr/src/app
ADD resources/docker /usr/src/app

RUN Rscript -e 'install.packages("vendor/bama", repos=NULL, type="source")'
RUN export RSCRIPT_BIN_PATH=`which Rscript`
RUN ./install_python.sh

CMD ["bash"]
