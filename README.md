# Sparkify Song Plays Data Lake

This pipeline receives json formatted song data and json formatted song play log events, transforming them to a
data lake of fact and dimensions.

## Why

Sparkify needs to be able to analyse what songs users are listening to.

## Transformation Assumptions

Artist data - to achieve uniqueness of record, we use the most recent data
for a given artist selected by year and longest song duration from the staging songs table.

User data - we use the most recent user data by log event timestamp when extracting a single
user.

## Schema Design

A star schema revolving around facts and dimensions is used here to achieve a data store
that is more flexible for analysis.

The fact table `songplays` can be joined with the dimensions: `time`, `artists`, `songs`, and `users` to constrain
data for analysis.

## Pipeline

We have pre-existing data sources in the form of `json` files in AWS S3 storage.

Our destination is a set of parquet files in AWS S3 to be used as a data lake, grouped under their respective directory
names.

## Run it

Populate a version of `dl.cfg` from the `dl.cfg.template` file.

Pass `etl.py` to your AWS Elastic Map Reduce (EMR) cluster.  How you initiate this will vary depending on your
connection options.
