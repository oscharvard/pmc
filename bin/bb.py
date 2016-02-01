#!/bin/env python3

"""
 bb = batch builder (inspired by DRS!)

 create bash commands to run pmc2dash process for a monthly batch.

input: target year and zero padded month delimited by underscore

/home/osc/proj/pmc/bin/bb.py 2013_05

example output:

mkdir -p /home/osc/proj/pmc/data/batch/pmc2014_04.2014_05_09/oai;
/home/osc/proj/ingest/bin/oai-harvest.py -u "http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&metadataPrefix=pmc_fm&from=2014-04-01&until=2014-04-30&set=pmc-open#" -d "/home/osc/proj/pmc/data/batch/pmc2014_04.2014_05_09/oai";
/home/osc/proj/pmc/bin/pmc2dash.py pmc2014_04.2014_05_09;
rsync -avz /home/osc/proj/pmc/data/batch/pmc2014_04.2014_05_09/import dspace@bishop.hul.harvard.edu:/home/dspace/import/pmc2014_04.2014_05_09/;

"""

import calendar
from datetime import date
import re
import sys

def main() :
    year_month = sys.argv[1]
    parts = year_month.split("_")
    year  = parts[0]
    month = parts[1]
    batch_start_date = re.sub("_","-",year_month +"_01")
    batch_end_date   = re.sub("_","-",year_month + "_" + str(calendar.monthrange(int(year),int(month))[1]))
    batch_run_date=str(date.today()).replace("-","_")
    batch_id = "pmc" + year_month + "." + batch_run_date

    osc_root = "/home/osc"
    pmc_proj_dir  = osc_root + "/proj/pmc"
    pmc_batch_dir = pmc_proj_dir + "/data/batch/" + batch_id
    pmc_oai_dir   = pmc_batch_dir + "/oai"
    #pmc_oai_url   = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&metadataPrefix=pmc_fm&from=' + batch_start_date + '&until=' + batch_end_date + '&set=pmc-open#'
    # changed 2016-02-01, make sure this works -- we've been seeing 301s for a long time, they may have turned off the original route.
    pmc_oai_url   = 'http://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=ListRecords&metadataPrefix=pmc_fm&from=' + batch_start_date + '&until=' + batch_end_date + '&set=pmc-open#'
    pmc_sip_dir   = pmc_batch_dir + "/import"

    print("mkdir -p " + pmc_oai_dir + ";")
    print(osc_root + '/proj/ingest/bin/oai-harvest.py -u "' + pmc_oai_url + '" -d "' + pmc_oai_dir + '";')    
    print( pmc_proj_dir + "/bin/pmc2dash.py " + batch_id +";")
    print("rsync -avz " + pmc_sip_dir + " dspace@turner.lib.harvard.edu:/home/dspace/import/" + batch_id + "/;")

main()



