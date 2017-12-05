#!/bin/env python3

import calendar
from argparse import ArgumentParser
from datetime import date
import re
import sys

ap = ArgumentParser(prog="""bb = batch builder (inspired by DRS!)

create bash commands to run pmc2dash process for a monthly batch.

example output:

    mkdir -p /home/osc/proj/pmc/data/batch/pmc2014_04.2014_05_09/oai;
    /home/osc/proj/ingest/bin/oai-harvest.py -u "'https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=ListRecords&metadataPrefix=pmc_fm&from=2014-04-01&until=2014-04-30&set=pmc-open#" -d "/home/osc/proj/pmc/data/batch/pmc2014_04.2014_05_09/oai";
    /home/osc/proj/pmc/bin/pmc2dash.py pmc2014_04.2014_05_09;
    rsync -avz /home/osc/proj/pmc/data/batch/pmc2014_04.2014_05_09/import dspace@byrd.lib.harvard.edu:/home/dspace/import/pmc2014_04.2014_05_09/;
""")

ap.add_argument("year_month", type=lambda x: x.split("_"), help="full year and zero-padded month, separated by an underscore, e.g. 2017_03")

def main() :
    args= ap.parse_args()
    year, month = args.year_month

    batch_start_date = "{}-{}-01".format(year, month)
    batch_end_date   = "{}-{}-{}".format(year, month, str(calendar.monthrange(int(year),int(month))[1]))
    batch_run_date=str(date.today()).replace("-","_")
    batch_id = "pmc{}_{}.{}".format(year, month, batch_run_date)

    osc_root = "/home/osc"
    pmc_proj_dir  = osc_root + "/proj/pmc"
    pmc_batch_dir = pmc_proj_dir + "/data/batch/" + batch_id
    pmc_oai_dir   = pmc_batch_dir + "/oai"

    pmc_oai_url   = 'https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=ListRecords&metadataPrefix=pmc_fm&from=' + batch_start_date + '&until=' + batch_end_date + '&set=pmc-open#'
    pmc_sip_dir   = pmc_batch_dir + "/import"

    print("mkdir -p " + pmc_oai_dir + ";")
    print(osc_root + '/proj/ingest/bin/oai-harvest.py -u "' + pmc_oai_url + '" -d "' + pmc_oai_dir + '";')
    print( pmc_proj_dir + "/bin/pmc2dash.py " + batch_id +";")
    print("rsync -avz " + pmc_sip_dir + " dspace@byrd.lib.harvard.edu:/home/dspace/import/" + batch_id + "/;")

main()
