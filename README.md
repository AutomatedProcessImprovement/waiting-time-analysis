# Waste Calculation

The project presents a tool to identify and calculate waste in a business process given an event log. The implementation is based on the thesis "[Waste Identification from Event Logs](https://comserv.cs.ut.ee/ati_thesis/datasheet.php?id=72411&year=2021)" by *Shefali Ajit Sharma* defended in the [University of Tartu](https://www.ut.ee/en) in 2021.  

## Installation

```shell
$ git clone https://github.com/AutomatedProcessImprovement/Waste-Calculation.git waste-calculation
$ git submodule update --init --recursive
$ cd waste-calculation
$ python3 -m venv venv
$ source venv/bin/activate
$ python -m pip install --upgrade pip
$ pip install -e .
```

## Getting Started

```shell
$ waste --log_path data/PurschasingExample.xes
```

The tool produces statistics in the Excel-format and saves it in the folder where the tool has been executed by default. 

See `waste --help` for more CLI options.
