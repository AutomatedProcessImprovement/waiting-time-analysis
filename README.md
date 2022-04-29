# Process Waste

![CI Status](https://github.com/AutomatedProcessImprovement/process-waste/actions/workflows/main.yml/badge.svg) [![Coverage Status](https://coveralls.io/repos/github/AutomatedProcessImprovement/pm4py-wrapper/badge.svg?branch=main)](https://coveralls.io/github/AutomatedProcessImprovement/pm4py-wrapper?branch=main)

A tool to identify waste in a business process from an event log.

## Installation

```shell
$ git clone https://github.com/AutomatedProcessImprovement/process-waste.git process-waste
$ cd process-waste
$ git submodule update --init --recursive
$ python3.9 -m venv venv
$ source venv/bin/activate
$ python -m pip install --upgrade pip
$ pip install .
$ cd start-time-estimator
$ pip install -e .
```

## Getting Started

```shell
$ process-waste transportation -l data/PurschasingExample.xes
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

## Contributing

For contributions, please install `pre-commit` to test the code before committing automatically.

```shell
$ pip install pre-commit
$ pre-commit install
```

Install `poetry` to manage dependencies and versioning.

```shell
$ python -m pip install poetry
$ poetry version minor # or patch, major, etc. to bump the version
$ poetry add <package> # to add a dependency
```

## References

- [Waste Identification from Event Logs](https://comserv.cs.ut.ee/ati_thesis/datasheet.php?id=72411&year=2021) by Shefali Ajit Sharma, [University of Tartu](https://www.ut.ee/en), 2021
