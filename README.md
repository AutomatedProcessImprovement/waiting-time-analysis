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
$ waste transportation -l data/PurschasingExample.xes -o results
```

The tool produces statistics in the Excel-format and saves it in the folder where the tool has been executed by default. 

See `waste --help` and `waste <cmd> --help` for more help.
