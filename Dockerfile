FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get -y install  \
    tzdata \
    python3.10 \
    python3-pip \
    python3.10-venv \
    git

RUN ln -fs /usr/share/zoneinfo/Europe/Tallinn /etc/localtime

WORKDIR /usr/src/app
ADD . /usr/src/app

RUN bash install_python.sh

CMD ["bash"]
