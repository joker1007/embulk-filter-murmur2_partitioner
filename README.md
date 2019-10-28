# Murmur2 Partitioner filter plugin for Embulk

Add partition number that is calculated by Apache Kafka's murmur2 partitioner.

## Overview

* **Plugin type**: filter

## Configuration

- **key_column**: column as partition key (string, required)
- **partition_column**: set partition number to this column. unless the column exists, add the column to output schema automatically. (string, default: `"partition"`)
- **partition_count**: Partition Count (integer, required)

## Example

```yaml
filters:
  - type: murmur2_partitioner
    key_column: id
    partition_count: 8
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
