#!/bin/env python3

# take pubmed batch.
# read batch input files
# for each harvard match, create a dc file 
# spit out to batch specific output directory.

import glob
import os
OSCROOT=os.environ['OSCROOT'] #/home/osc

import random
import re
import shutil
import sys
sys.path.append(OSCROOT + '/proj/ingest/lib')
import bulklib
sys.path.append(OSCROOT + '/common/lib/python3')
import time
import tsv
import xml.etree.ElementTree as etree  
from urllib.request import FancyURLopener
import urllib.request, urllib.parse, urllib.error

PUBMED_DIR=OSCROOT + "/proj/pubmed"
DATA_DIR  =PUBMED_DIR + "/data"

def main() :
    batch = sys.argv[1]
    print("Processing batch: " + batch)
    batch_oai = DATA_DIR + "/batch/"+batch+"/oai"
    print("Batch input oai dir: " + batch_oai)
    batch_out_dir =  DATA_DIR+"/batch/"+batch+"/import"
    prep_batch_out_dir(batch_out_dir)
    article_number=0
    report=init_report(batch)

    for oai_file in glob.glob( os.path.join(batch_oai, '*.xml') ):
        print("current file is: " + oai_file)
        report['oai_pages']+=1
        tree = etree.parse(oai_file)
        root = tree.getroot()
        fas_depts   = bulklib.load_fas_departments()
        dash_dois   = bulklib.load_dash_dois()
        dash_titles = bulklib.load_dash_titles()
        dash_pmcids = bulklib.load_dash_pmcids()
        for article_node in tree.findall('.//{http://www.openarchives.org/OAI/2.0/}metadata') :
            report['articles_total']+=1
            if is_harvard_article_node(etree,article_node) :
                report['articles_harvard']+=1
                article = extract_article(article_node)
                assign_article_schools(article,fas_depts)
                if has_valid_school(article) :
                    if not already_in_dash(article,dash_dois,dash_titles,dash_pmcids):
                        download_files(article,batch)
                        if len(article['files']) > 0 :
                            set_license(article)
                            write_output(batch,batch_out_dir,article, article_number)
                            article_number +=1
                            report['articles_loaded']+=1
                        else :
                            report['articles_error_no_files']+=1
                    else :
                        report['articles_already_in_dash']+=1
                else :
                    report['articles_error_no_valid_school']+=1
    print_report(report)


def init_report(batch) :
    report={}
    report['batch']=batch
    report['oai_pages']=0
    report['articles_total']=0
    report['articles_harvard']=0
    report['articles_error_no_valid_school']=0
    report['articles_error_no_files']=0
    report['articles_already_in_dash']=0
    report['articles_loaded']=0
    return report

def print_report(report) :
    print(report)


def kvor(dict,key,default):
    if key in dict :
        if dict[key] != None :
            return dict[key]
    return default


def findall_texts(node,tag) :
    texts = []
    for subnode in findall(node,tag) :
        texts.append(subnode.text)
    return texts


def findall(node,tag):
    return node.findall('.//{http://dtd.nlm.nih.gov/2.0/xsd/archivearticle}'+tag) 


def find(node,tag):
    return node.find('.//{http://dtd.nlm.nih.gov/2.0/xsd/archivearticle}'+tag) 


def find_attrib(node,tag,attrib,value) :
    for subnode in findall(node,tag) :
        if subnode.attrib[attrib] == value :
            return subnode


def findall_attrib(node,tag,attrib,value) :
    foundnodes = []
    for subnode in findall(node,tag) :
        if attrib in subnode.attrib and subnode.attrib[attrib] == value :
            foundnodes.append(subnode)
    return foundnodes


def is_harvard_article_node(etree,article_node) :
    # quick preliminary screen: does this article node have a harvard affiliated contributor?
    # note: does not weed out editors.
    for aff_node in findall(article_node,'aff') :
        affBytes = etree.tostring(aff_node,"utf-8")
        affString = str(affBytes)
        if re.search("harvard",affString.lower()) :
            print(affString)
            return True
    return False


def extract_affs(article_node):
    affs = []
    for aff_node in findall(article_node,'aff') :
        aff = {}
        aff['id']= kvor(aff_node.attrib,'id',None)
        settext(aff,'sup',find(aff_node,'sup'))
        aff['text']= catnode(aff_node)
        affs.append(aff)
    return affs


def extract_aff_ids(author_node) :
    rids = []
    for xref_node in findall_attrib(author_node,"xref","ref-type","aff") :
        rids.append(xref_node.attrib['rid'])
    return rids


def extract_authors(article_node,affs) :
    authors = []
    #contrib contrib-type="author"
    # we filter out editors here.
    for author_node in findall_attrib(article_node,'contrib','contrib-type','author') :
        author = {}
        settext(author,'last',find(author_node,'surname'))
        settext(author,'first',find(author_node,'given-names'))
        #print("REINOS:author")
        if len(affs) == 1 :
            # there's just one possible aff. 
            author['affs'] = affs
        else :
            # there are multiple possible affs.
            author['affs']=[]
            author['aff_ids']=extract_aff_ids(author_node)
            for aff in affs :
                for aff_id in author['aff_ids'] :
                    #print("REINOS: DOES " + aff_id + " = " + aff['id'] + "?")
                    if aff_id == aff['id'] :
                        author['affs'].append(aff)
        if len(author['affs']) == 0 :
            print("UNABLE TO EXTRACT AFF INFO FROM AUTHOR:")
            print(author)
            print("AFFS: ")
            print(affs)
            print("I think that's OK. As long as one of the article authors has an affilication.")
            #exit()
        if 'last' in author :
            authors.append(author)
        #print(author)
    return authors

def settext(object,key,node) :
    # find a better name for this. 
    if node != None :
        object[key]=node.text

def extract_article(article_node) :
    # take pubmed xml article node and create article object.
    article = {}
    pmcid = None
    article['title']=catnode(find(article_node,'article-title')) #.text or "Untitled"
    article['journal']=find(article_node,'journal-title').text
    article['type']='Journal Article'
    article['pmcid']=find_attrib(article_node,'article-id','pub-id-type','pmc').text
    settext(article,'doi',find_attrib(article_node,'article-id','pub-id-type','doi'))
    article['files']=[]
    article['issn']=find(article_node,'issn').text
    #article['subjects']=findall_texts(article_node,'subject')
    article['subjects']=extract_subjects(article_node)
    settext(article,'publisher',find(article_node,'publisher-name'))
    article['abstract']=extract_abstract(article_node)
    settext(article,'date',find(article_node,'copyright-year'))
    if 'date' not in article :
        article['date']=find(article_node,'year').text
    for key in ( 'volume','issue','fpage','lpage' ) :
        settext(article,key,find(article_node,key))
    article['affs']=extract_affs(article_node)
    article['authors']=extract_authors(article_node,article['affs'])
    article['citation']=build_citation(article)
    return article


def assign_article_schools(article,fas_depts) :
    # primitive assignment of article to schools based on author affiliation string.
    schoolMap = {}
    deptMap = {}
    for author in article['authors'] :
        for aff in author['affs'] :
            afftext=aff['text'].lower()
            if re.match(".*(harvard medical school|harvard university school of medicine|massachusetts general hospital).*",afftext) :
                schoolMap['HMS']=1
            elif re.match(".*harvard school of public health.*",afftext) :
                schoolMap['HSPH']=1
            elif re.match(".*harvard graduate school of education.*",afftext) :
                schoolMap['GSE']=1
            elif re.match(".*harvard university.*",afftext) :
                print("FAS afftext: " + afftext)
                print(author)
                for dept in fas_depts :
                    if re.match(".*"+ dept.lower() +".*",afftext) :
                        deptMap[dept]=1
                        break
                if len(deptMap) > 0 :
                    schoolMap['FAS']=1
                else :
                    schoolMap['FAS?']=1
            elif re.match(".*harvard.*",afftext) :
                schoolMap['harvard?']=1
    article['schools']=list(schoolMap.keys())
    article['departments']=list(deptMap.keys())
    print("schools:")
    print(article['schools'])
    print("departments:")
    print(article['departments'])


def set_license(article) :
    if 'FAS' in article['schools'] or 'GSE' in article['schools'] or 'HLS' in article['schools']:
        article['license'] = 'OAP'
    else :
        article['license'] = 'LAA'


def build_citation(article) :
    citation=""
    i =1
    for author in article['authors'] :
        if author['first'] == None :
            # it's not a person, but a consortium or something -- no first name.
            citation+= author['last']
        elif i == 1 :
            citation+= author['last'] + ", "
            citation+= author['first'] + ", "
        else :
            if i == len(article['authors']) :
                citation += "and "
            citation+= author['first'] + " "
            citation+= author['last'] + ", "
        i+=1
    citation += ". " + article['date'] 
    citation += ". " + article['title'][0] + article['title'][1:].lower() + ". "
    citation += article['journal'] + " "
    citation = re.sub(", \.", ".", citation)
    citation = re.sub("\.\.",".",citation)
    citation = re.sub("\s+"," ",citation)
    if 'volume' in article :
        citation += article['volume']
    if 'issue' in article :
        citation += "(" + article['issue'] + ")"
    if 'fpage' in article :
        fpage  = article['fpage']
        pages = ': ' + fpage
        if 'lpage' in article :
            lpage  = article['lpage']
            if lpage != None and fpage != lpage:
                pages = ': ' + fpage + '-' + lpage
        citation += pages
    else :
        pages = ""
    citation += '.'
    return citation


def extract_subjects(article_node) :
    # really irritating. there are all these "heading" subjects.
    subjects = []
    for subject_group_node in findall(article_node,'subj-group') :
        if 'subj-group-type' in subject_group_node.attrib :
            if subject_group_node.attrib['subj-group-type'] == 'heading' :
                continue
        for subject_node in findall(subject_group_node,'subject') :
            # crappy "heading" subjects still sometimes get through.
            if not re.match("Research|Letter|Communication|Dispatch|Tools|Original Research|\d+",subject_node.text) :
                if subject_node.text not in subjects :
                    subjects.append(subject_node.text)
    return subjects


def extract_text(node) :
    # get concatenated text string for all subnodes if any.
    text=""
    for t in abstract_node.itertext() :
        text+=t
    return text


def catnode(node):
    # concatenate text for all subnotes, with whitespace cleanup.
    cattext=""
    for text in node.itertext() :
        text = re.sub("\s+"," ",text)
        cattext+=text
    return cattext.strip()


def extract_abstract(article_node):
    # complex because in a bunch of subnodes. this method is crude.
    abstract_node =find(article_node,'abstract')
    abstract = ""
    if abstract_node :
        for text in abstract_node.itertext() :
            text = re.sub("\s+"," ",text)
            abstract+=text
            if re.match("Background|Results|Introduction|Conclusion|Conclusions|Objective|Case presentation",text) :
                # a bit hacky. should really add colon after "title" elements in abstact.
                abstract+=":"
    return abstract.strip()


def already_in_dash(article,dash_dois,dash_titles,dash_pmcids) :
    # first cut at duplicate detection.
    found = False
    if 'doi' in article and article['doi'] in dash_dois :
        print("BAM! Found DOI: " + article['title'])
        found = True
    if article['pmcid'] in dash_pmcids :
        print("BAM! Found pmcid: " + article['pmcid'])
        found = True
    if article['title'] in dash_titles :
        print("BAM! Found title: " + article['title'])
        found = True
    return found


def has_valid_school(article) :
    for school in article['schools'] :
        if not re.match(".*\?$",school) :
            return True
    return False


def get_target_collection(article):
    collection = ""
    for school in sorted(article['schools']) :
        if not re.match(".*\?$",school) :
            collection += school
            collection += "_"
    collection = re.sub("_$","",collection)
    return collection


def download_files(article,batch):
    # TODO: replace with FTP
    # TODO: handle non-pdf articles
    # TODO: can there be multiple files?
    pdf_url = "http://www.ncbi.nlm.nih.gov/pmc/articles/PMC" + article['pmcid'] + "/pdf/"
    article['hasversion']=pdf_url
    file = {}
    file['url'] = pdf_url
    file['name'] = article['pmcid']+'.pdf'
    cachepath = DATA_DIR + "/batch/"+batch+"/articles"
    if not os.path.exists(cachepath) :
        os.mkdir(cachepath)
    file['cachepath']  =  cachepath+ "/" +file['name']
    errorpath = file['cachepath'] + ".error"
    if ( os.path.exists(file['cachepath']) or os.path.exists(errorpath) ) :
        print("Article file in cache...");
    else :
        opener = FancyURLopener({})
        print("Downloading " + file['url'] + " to " + file['cachepath'])
        request = urllib.request.Request(file['url'])
        request.add_header('User-Agent','Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.53 Safari/534.3')
        try :
            f = urllib.request.urlopen(request)
            local_file = open(file['cachepath'], "wb")
            local_file.write(f.read())
            local_file.close()
        except urllib.error.URLError as e:
            local_file = open(errorpath, "w")
            local_file.write('Error getting url:\n'+file['url'])
            local_file.write('Code: ' + str(e.code))
            local_file.write('Read: ' + str(e.read))
            local_file.close()
        time.sleep(random.randint(3, 6))
    if os.path.exists(file['cachepath']) :
        article['files'].append(file)


def write_output(batch,batch_out_dir,article,article_number) :
    #print("Batch: " + batch)
    target_collection = get_target_collection(article) 
    collection_out_dir = batch_out_dir + "/" + target_collection
    article_out_dir= collection_out_dir +"/"+str(article_number)
    if not os.path.exists(batch_out_dir) :
        os.mkdir(batch_out_dir)
    if not os.path.exists(collection_out_dir) :
        os.mkdir(collection_out_dir)
    if not os.path.exists(article_out_dir) :
        os.mkdir(article_out_dir)
    #print(article)
    bulklib.write_dublin_core_meta(article,article_out_dir,batch)
    bulklib.write_dash_meta(article,article_out_dir)
    bulklib.write_contents_file(article,article_out_dir)
    for file in article['files'] :
        shutil.copyfile(file['cachepath'], article_out_dir + "/" + file['name'])
    shutil.copyfile(DATA_DIR+"/licenses/" + article['license'] + '/license.txt', article_out_dir+'/license.txt')

def prep_batch_out_dir(batch_out_dir) :
    if os.path.exists(batch_out_dir) :
        print("Deleting existing batch_out_dir: " + batch_out_dir)
        shutil.rmtree(batch_out_dir)
    print("Creating batch_out_dir: " + batch_out_dir)
    os.mkdir(batch_out_dir)


main()




