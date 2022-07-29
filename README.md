# Waiting Time Analysis of Activity Transitions

![CI Status](https://github.com/AutomatedProcessImprovement/waiting-time-analysis/actions/workflows/main.yml/badge.svg)

A tool to identify waste in a business process from an event log.

## Installation

_Rscript_ should be installed on your system.

```shell
$ git clone https://github.com/AutomatedProcessImprovement/waiting-time-analysis.git waiting-time-analysis
$ cd waiting-time-analysis
$ export RSCRIPT_BIN_PATH=/usr/bin/Rscript # put your Rscript path here
$ bash install.sh # installs R packages and Python dependencies
```

## Getting Started

```shell
$ wta -l tests/assets/PurschasingExample.csv
```

The tool produces statistics in the CSV-format and saves it in the folder where the tool has been executed by default. 

## Usage

See `wta --help` and `wta <cmd> --help` for more help.

```
Usage: wta [OPTIONS]

Options:
  -l, --log_path PATH    Path to an event log in XES-format.  [required]
  -o, --output_dir PATH  Path to an output directory where statistics will be
                         saved.  [default: ./]
  -p, --parallel         Run the tool using all available cores in parallel.
                         [default: True]
  --help                 Show this message and exit.
```

## Docker

    
```shell
$ docker pull nokal/waiting-time-analysis
$ docker run -v $(pwd)/data:/usr/src/app/data nokal/waiting-time-analysis wta -l /usr/src/app/data/<event_log_name.csv> -o /usr/src/app/data
```

## Contributing

For contributions, please install `pre-commit` to test the code before committing automatically.

```shell
$ pip install pre-commit
$ pre-commit install
```

## References

- [Waste Identification from Event Logs](https://comserv.cs.ut.ee/ati_thesis/datasheet.php?id=72411&year=2021) by Shefali Ajit Sharma, [University of Tartu](https://www.ut.ee/en), 2021
