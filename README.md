# PubMedCentral Batch Ingest scripts #

This is a collection of scripts to support batch ingest of articles published to PMC into Dash

When they're at home, these scripts live in the ~/proj/pmc/bin directory of the data analysis server (bayes at time of last update).

## Scripts
- *bb.py* - "batch builder" which prints create bash commands to be run manually to execute pmc2dash process for a monthly batch.
- *pmc2dash.py* - takes the input from the oai-pmh harvest, and for each harvard match, create a dc file in batch output dir

Note that there are commands not in this repo that are part of batch creation.