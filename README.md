# s3-data-watcher
Watch S3 data events

## Build
Type `make` to build the project.

Binary file will be created under `./bin` directory.

## Run
You need two files `config.yaml` and `jobs.yaml`.

Run using following command.
```bash
./bin/s3-data-watcher -f -c config.yml
```