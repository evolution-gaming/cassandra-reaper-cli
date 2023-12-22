# Cassandra Reaper CLI

[![PyPI - Version](https://img.shields.io/pypi/v/cassandra-reaper-cli.svg)](https://pypi.org/project/cassandra-reaper-cli)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cassandra-reaper-cli.svg)](https://pypi.org/project/cassandra-reaper-cli)

-----

**Table of Contents**

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install cassandra-reaper-cli
```

## Usage
```console
$ reaper -h
usage: reaper [-h] [--url URL] [--username USERNAME] [--password PASSWORD] [--disable-ssl-verify]
              {schedule-list,schedule-info,schedule-disable,schedule-enable,schedule-start,schedule-delete,repair-list,repair-info,repair-pause,repair-abort,repair-delete,repair-resume,repair-intensity-change,repair-segment-list,repair-segment-abort,cluster-list,cluster-table-list,cluster-schedule-list,cluster-schedule-enable,cluster-schedule-disable,cluster-schedule-delete,cluster-repair-list,cluster-repair-pause,cluster-repair-resume,cluster-repair-abort,cluster-repair-delete,cluster-enable,cluster-disable,completion-print}
              ...

Cassandra Reaper CLI

positional arguments:
  {schedule-list,schedule-info,schedule-disable,schedule-enable,schedule-start,schedule-delete,repair-list,repair-info,repair-pause,repair-abort,repair-delete,repair-resume,repair-intensity-change,repair-segment-list,repair-segment-abort,cluster-list,cluster-table-list,cluster-schedule-list,cluster-schedule-enable,cluster-schedule-disable,cluster-schedule-delete,cluster-repair-list,cluster-repair-pause,cluster-repair-resume,cluster-repair-abort,cluster-repair-delete,cluster-enable,cluster-disable,completion-print}
                        Supported commands
    schedule-list       Repair Schedule list
    schedule-info       Information about Schedule by ID
    schedule-disable    Disable Repair Schedule by ID
    schedule-enable     Enable Repair Schedule by ID
    schedule-start      Start Repair Schedule by ID
    schedule-delete     Delete Repair Schedule by ID
    repair-list         Repairs list
    repair-info         Information about Reapair by ID
    repair-pause        Pause running repair by ID
    repair-abort        Abort repair by ID
    repair-delete       Delete repair by ID
    repair-resume       Resume paused repair by ID
    repair-intensity-change
                        Change PAUSED repair intensity
    repair-segment-list
                        List repair segments by ID
    repair-segment-abort
                        Aborts a running segment and puts it back in NOT_STARTED state. The segment will be processed again later during the lifetime of the repair run
    cluster-list        Clusters list
    cluster-table-list  Tables list of a Cluster (keyspace)
    cluster-schedule-list
                        Repair Schedules list of a Cluster
    cluster-schedule-enable
                        Enable all repair schedules of a cluster
    cluster-schedule-disable
                        Disable all repair schedules of a cluster
    cluster-schedule-delete
                        Disable and delete all repair schedules of a cluster
    cluster-repair-list
                        Cluster Repairs list
    cluster-repair-pause
                        Pause all running repairs of a cluster
    cluster-repair-resume
                        Resume all paused repairs of a cluster
    cluster-repair-abort
                        Abort all running and paused repairs of a cluster
    cluster-repair-delete
                        Delete all repairs of a cluster
    cluster-enable      Enable all repair schedules and resume all paused repairs of a cluster
    cluster-disable     Disable all repair schedules and pause all running repairs of a cluster
    completion-print    Print completion for shell

optional arguments:
  -h, --help            show this help message and exit
  --url URL             Reaper URL (default value from env REAPER_URL)
  --username USERNAME   Reaper username (default value from env REAPER_USER)
  --password PASSWORD   Reaper password (default value from env REAPER_PASSWORD)
  --disable-ssl-verify  Disable SSL verification
```

## License

`cassandra-reaper-cli` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
