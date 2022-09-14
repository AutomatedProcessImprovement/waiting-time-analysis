# Waiting Time Analysis of Activity Transitions

![CI Status](https://github.com/AutomatedProcessImprovement/waiting-time-analysis/actions/workflows/main.yml/badge.svg)

Implementation of the technique presented in the paper "*Why am I Waiting? Data-Driven Analysis of Waiting Times in Business Processes*", by
Katsiaryna Lashkevich, Fredrik Milani, David Chapela-Campa, Ihar Suvorau and Marlon Dumas.

The technique takes as input an event log, in csv format wherein each row represents the execution of an activity (with start and end 
timestamps), and produces a report with the waiting time of each activity transition (e.g. from activity A to activity B) classified 
into five different categories.
- Waiting time due to **batch processing**: the activity instance is waiting until a set of instances of the same activity are accumulated and executed together.
- Waiting time due to **resource contention**: the resource that has to perform this activity is busy processing another activity.
- Waiting time due to **prioritization**: the resource that has to perform this activity is busy because they prioritized another activity that was supposed to be executed later.
- Waiting time due to **resource unavailability**: the resource is not working at that specific moment (e.g. non working weekend).
- Waiting time due to **extraneous factors**: the activity instance waits because of causes unrelated with the available data (e.g. the resource is busy working in another project, or the activity has to wait for an external event to happen).

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
