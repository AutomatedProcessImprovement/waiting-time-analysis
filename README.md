# Process Waste

A tool to identify waste in a business process from an event log.

## Installation

```shell
$ git clone https://github.com/AutomatedProcessImprovement/Waste-Calculation.git process-waste
$ cd process-waste
$ git submodule update --init --recursive
$ python3 -m venv venv
$ source venv/bin/activate
$ python -m pip install --upgrade pip
$ pip install poetry
$ poetry install
$ cd start-time-estimator
$ pip install -e .
```

## Getting Started

```shell
$ process-waste transportation -l data/PurschasingExample.xes -o results
```

The tool produces statistics in the CSV-format and saves it in the folder where the tool has been executed by default. 

## Usage

See `process-waste --help` and `process-waste <cmd> --help` for more help.

```
Usage: process-waste [OPTIONS] COMMAND [ARGS]...

  Grouping commands under main group.

Options:
  --help  Show this message and exit.

Commands:
  transportation
```

## References

- [Waste Identification from Event Logs](https://comserv.cs.ut.ee/ati_thesis/datasheet.php?id=72411&year=2021) by Shefali Ajit Sharma, [University of Tartu](https://www.ut.ee/en), 2021
