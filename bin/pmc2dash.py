#!/bin/env python3

# take pubmed central (pmc) batch.
# read batch input files
# for each harvard match, create a dc file
# spit out to batch specific output directory.

import sys, os

OSCROOT=os.environ['OSCROOT'] #/home/osc

sys.path.append(OSCROOT + '/proj/ingest/lib')
sys.path.append(OSCROOT + '/common/lib/python3')

import argparse, glob, json, random, re, shutil, bulklib, time, tsv
import urllib.request, urllib.parse, urllib.error
from pprint import pprint
from lxml import etree

AUTHORITY_REPORT=[]

PMC_DIR=OSCROOT + "/proj/pmc"
DATA_DIR  =PMC_DIR + "/data"
UNAFFILIATED = 'UNAFFILIATED'

DASH2LDAP_SCHOOL = bulklib.load_dash2ldap_school()
LDAP2DASH_SCHOOL = {v:k for k, v in DASH2LDAP_SCHOOL.items()}

OAI_NS = None
ARTICLE_NS = None

def main():
    parser = argparse.ArgumentParser(description='''Processor for pubmed central (pmc) batch.
    1. read batch input files
    2. for each Harvard match, create a dc file
    3. spit out to batch specific output directory.''')
    parser.add_argument('batch', metavar='BATCH', help='name of the base directory for the batch')
    args = parser.parse_args()
    batch = args.batch

    print("Processing batch: " + batch)

    base_dir = "{}/batch/{}".format(DATA_DIR, batch)
    print("Base Directory: " + base_dir)

    batch_out_dir = base_dir + "/import"
    report_dir =    base_dir + "/report"

    prep_batch_out_dir(batch_out_dir)

    article_number = 0
    report = init_report(batch)

    fas_depts   = bulklib.load_fas_departments()
    dash_dois   = bulklib.load_dash_dois()
    dash_titles = bulklib.load_dash_titles()
    dash_pmcids = bulklib.load_dash_pmcids()

    for oai_file in glob.glob( os.path.join(base_dir, "oai", '*.xml') ):
        print("current file is: " + oai_file)
        report['oai_pages']+=1
        tree = etree.parse(oai_file)

        # Get default namespaces out of the document - we've had issues with the article NS switching from HTTP to HTTPS
        global OAI_NS, ARTICLE_NS

        if not OAI_NS:
            OAI_NS = dict(tree.xpath('/*/namespace::*')).get(None, 'http://www.openarchives.org/OAI/2.0/')
        if not ARTICLE_NS:
            ARTICLE_NS = dict(tree.xpath('//*[local-name(.) = "article"]/namespace::*')).get(None, 'https://jats.nlm.nih.gov/ns/archiving/1.0/')

        for article_node in tree.findall('.//{{{}}}metadata'.format(OAI_NS)):
            report['articles_total']+=1
            if is_harvard_article_node(etree,article_node) :
                report['articles_harvard'] += 1
                article = extract_article(article_node)
                assign_article_schools(article,fas_depts)
                attach_authorities(article)
                for author in article['authors'] :
                    if author['has_harvard_affstring'] :
                        update_harvard_author_counts(report,author)
                update_harvard_article_counts(report,article)
                in_dash = already_in_dash(article,dash_dois,dash_titles,dash_pmcids)
                if not in_dash :
                    target_collection_dir = get_target_collection_dir(article)
                    print("REINOS: target_collection_dir: " + target_collection_dir)
                    if target_collection_dir == '' :
                        report['articles_error_no_valid_school'] += 1
                        print("REINOS: no valid LDAP or PMC school!")
                        print("REINOS: LDAP schools:"+str(article['ldap_schools']))
                        print("REINOS: PMC schools:"+str(article['pmc_schools']))
                    else :
                        download_files(article,batch)
                        if len(article['files']) > 0 :
                            # create dspace import packages for stuff that we were able to find harvard authority codes for.
                            # and has files and is not already in dash.
                            article['license'] = 'LAA' # as of Feb 2014 per Colin and Becky, license always == LAA
                            write_output(batch,batch_out_dir,article, article_number)
                            article_number += 1
                            report['articles_loaded']+= 1
                        else :
                            report['articles_error_no_files']+= 1
                else :
                    report['articles_already_in_dash']+= 1

    if not os.path.exists(report_dir) :
        os.mkdir(report_dir)
    print_report(report)
    write_author_report(report_dir)

def write_author_report(report_dir):
    pmcid2dashid = tsv.read_map(OSCROOT + '/proj/ingest/data/tsv/pmcid2dashid.tsv')
    jsondata={}
    jsondata['data']=[]
    jsondata['timestamp']='2014-02-20 19:36:32'
    for AR in AUTHORITY_REPORT :
        pa = AR['pmc_author']
        affstring = "|".join(aff['text'] for aff in pa.get('affs', []))

        la={'label':'NO MATCH', 'confidence' : 0.0}

        dashid=""
        if AR['pmcid'] in pmcid2dashid :
            dashid = '<a href="{0}">{0}</a>'.format(pmcid2dashid[AR['pmcid']])
        if 'best_match_author' in AR :
            la = AR['best_match_author']
        jsonrow = ('<a href="http://www.ncbi.nlm.nih.gov/pmc/articles/PMC{0}">{0}</a>'.format(AR['pmcid']),
                   dashid,
                   AR['title'],
                   pa['first'],
                   pa['last'],
                   affstring,
                   '<a href="{}">{}</a>'.format(AR['json_url'], len(AR['ldap_authors'])),
                   la['label'],
                   la['confidence'],)
        jsondata['data'].append(jsonrow)
    with open(report_dir+"/author-report.json", "wb") as f:
        f.write(bytes(json.dumps(jsondata), 'UTF-8'))

def update_harvard_article_counts(report,article):
    if article['found_all_harvard_auths']:
        report['found_all_harvard_auths'] += 1
    if article['found_any_harvard_auths']:
        report['found_any_harvard_auths'] += 1
    else:
        report['found_no_harvard_auths'] += 1

def update_harvard_author_counts(report,author):
    report['harvard_authors_count'] += 1
    if author['authority'] != UNAFFILIATED:
        report['harvard_authors_matched_count'] += 1
    if author['match_count'] == 0:
       report['harvard_authors_no_matches_count'] += 1
    elif author['match_count']==1:
       report['harvard_authors_single_match_count'] += 1
    elif author['match_count'] > 1:
       report['harvard_authors_multiple_matches_count'] += 1

def init_report(batch):
    '''Initialize and return report dict with zeroes and batch name.'''
    report = {key:0 for key in  (
        'oai_pages', 'oai_pages', 'articles_total', 'articles_harvard', 'articles_error_no_valid_school', 'articles_error_no_files',
        'articles_already_in_dash', 'articles_loaded', 'found_all_harvard_auths', 'found_any_harvard_auths', 'found_no_harvard_auths',
        'harvard_authors_single_match_count', 'harvard_authors_matched_count', 'harvard_authors_multiple_matches_count', 'harvard_authors_no_matches_count',
        'harvard_authors_count', # aff string says harvard.
    )}
    report['batch'] = batch
    return report

def print_report(report):
    for key in sorted(report.keys()):
        print(key+": " + str(report[key]))


def findall(node,tag):
    return node.findall('.//article:{}'.format(tag), namespaces={'article': ARTICLE_NS})


def find(node,tag):
    return node.find('.//article:{}'.format(tag), namespaces={'article': ARTICLE_NS})


def find_attrib(node,tag,key,value) :
    for subnode in findall(node,tag) :
        if key in subnode.attrib and subnode.attrib[key] == value :
            return subnode


def findall_attrib(node,tag,key,value) :
    foundnodes = []
    for subnode in findall(node,tag) :
        if key in subnode.attrib and subnode.attrib[key] == value :
            foundnodes.append(subnode)
    return foundnodes


def is_harvard_article_node(etree,article_node):
    # quick preliminary screen: does this article node have a harvard affiliated contributor?
    # note: does not weed out editors.
    for aff_node in findall(article_node,'aff'):
        affString = str(etree.tostring(aff_node, encoding="utf-8"))

        if re.search("harvard",affString.lower().replace("harvard\.edu","").replace("harvard ave","")):
            print(affString)
            return True
    return False


def extract_affs(article_node):
    affs = []
    for aff_node in findall(article_node,'aff') :
        aff = {}
        aff['id']= aff_node.attrib.get('id',None)
        settext(aff,'sup',find(aff_node,'sup'))
        aff['text']= catnode(aff_node)
        affs.append(aff)
    return affs


def extract_aff_ids(author_node) :
    '''Extract aff_ids from xrefs within author node.'''
    return [xref_node.attrib['rid'] for xref_node in findall_attrib(author_node,"xref","ref-type","aff")]


def extract_authors(article_node,affs):
    authors = []
    #contrib contrib-type="author"
    # we filter out editors here.
    for author_node in findall_attrib(article_node,'contrib','contrib-type','author') :
        author = {'has_harvard_affstring': False,
                  'authority': UNAFFILIATED,
                  'match_count': 0,
                  'ldap_schools': set()}

        settext(author, 'last',  find(author_node,'surname'))
        settext(author, 'first', find(author_node,'given-names'))

        if len(affs) == 1:
            # there's just one possible aff.
            author['affs'] = affs
        else:
            # there are multiple possible affs.
            author['affs']=[]
            author['aff_ids']=extract_aff_ids(author_node)
            for aff in affs :
                for aff_id in author['aff_ids'] :
                    if aff_id == aff['id'] :
                        author['affs'].append(aff)
        if len(author['affs']) == 0:
            # I think that's OK. As long as one of the article authors has an affiation. - Ben
            print("UNABLE TO EXTRACT AFF INFO FROM AUTHOR: {}".format(author))
            print("AFFS: {}".format(affs))

        if 'last' in author :
            newAuthor = True
            for a in authors :
                if a['last'] == author['last'] and a.get('first', False) == author.get('first', False) :
                    print("REINOS: WOAH: AUTHOR DUPLICATION ******************************************")
                    pprint(a)
                    pprint(author)
                    newAuthor=False
                    break
            if newAuthor :
                authors.append(author)
    return authors

def settext(object,key,node):
    # find a better name for this.
    if node != None :
        if node.text != None :
            object[key]=node.text

def extract_type(article_node) :
    for subj_group_node in findall_attrib(article_node,'subj-group','subj-group-type','heading') :
        for subject_node in findall(subj_group_node,'subject') :
            #print("REINOS: Found TYPE info in subj-group-heading subject: " + subject_node.text)
            try:
                if re.match("Poster Presentation|Editorial",subject_node.text) :
                    #print("REINOS: this does not look like a research article")
                    return 'Other'
            except:
                pass
    return 'Journal Article'

def extract_pmcid(article_node):
    '''Get pmc id from article.

    Note: Location and formatting of PMCID in document has changed several times.'''
    return find_attrib(article_node,'article-id','pub-id-type','pmc-uid').text

def extract_article(article_node) :
    '''Take pmc xml article node and create simplified article object.'''
    article = {'found_all_harvard_auths': False,
               'found_any_harvard_auths': False,
               'title':           catnode(find(article_node,'article-title')), #.text or "Untitled"
               'journal':         find(article_node,'journal-title').text,
               'type':            extract_type(article_node),
               'pmcid':           extract_pmcid(article_node),
               'files':           [],
               'issn':            find(article_node,'issn').text,
               'subjects':        extract_subjects(article_node),
               'abstract':        extract_abstract(article_node),
               'affs':            extract_affs(article_node),
               'version':         'Version of Record',
               'ldap_schools':    set(),
               'pmc_schools':     set(),
               'pmc_depts':       set(),
               'harvard_authors': [],
    }
    print("REINOS: title: " + article['title'])

    subtitle_node = find(article_node,'subtitle')
    if subtitle_node is not None:
        print("FOUND SUBTITLE!")
        article['title'] += ": " + catnode(subtitle_node)
    else :
        print("Did NOT find subtitle!")

    article['authors'] = extract_authors(article_node,article['affs'])

    settext(article,'doi',find_attrib(article_node,'article-id','pub-id-type','doi'))
    settext(article,'publisher',find(article_node,'publisher-name'))

    for key in ( 'volume','issue','fpage','lpage','elocation-id' ) :
        settext(article,key,find(article_node,key))

    settext(article,'date',find(article_node,'copyright-year'))
    if 'date' not in article:
        article['date'] = find(article_node,'year').text

    article['citation']=build_citation(article)
    return article


def found_any_harvard_auths(article):
    return any(author for author in article['authors'] if author['has_harvard_affstring'] and author['authority'] != UNAFFILIATED)

def found_all_harvard_auths(article):
    for author in article['authors'] :
        if author['has_harvard_affstring']:
            if author['authority'] == UNAFFILIATED:
                return False
    return True

def attach_authorities(article):
    base_url = 'https://dash.harvard.edu/getBestMatch?format=json&'

    enc = urllib.parse.quote_plus
    harvard_author_count=0
    for author in article['authors']:
        if author['has_harvard_affstring']:
           harvard_author_count+=1
    harvard_author_index=0
    for author in article['authors']:
        if not author['has_harvard_affstring']:
            continue

        harvard_author_index += 1
        AR={ 'title': article['title'],
             'pmcid': article['pmcid'],
             'harvard_author_count': harvard_author_count,
             'harvard_author_index': harvard_author_index,
             'all_authors': len(article['authors']),
             'pmc_author': author
        }
        AUTHORITY_REPORT.append(AR)

        title_value=""
        afftexts=""
        for afftext in author['afftexts']:
            print("REINOS: afftext: " + afftext)
            afftext = re.sub('^[ß1234567890]','',afftext)
            afftexts+=afftext
            title_value+=afftext


        school_value = ",".join(author['pmc_schools'])

        first = author.get('first', False)
        if first:
            nameparts = author['first'].split(" ")
            first = nameparts[0]

        middle = ""
        last = author['last'] #.split(" ")[-1] # hack von whatever. remove split.
        #last = re.sub("-","",last)# hack to test without hyphens breaking it.
        # match the pmc aff string, scrubbed of some stopwords, against the title field in ldap.

        # temporary hack to deal with Peter aka Phillip Kraft
        if first == 'Peter' and last == 'Kraft' :
            first = 'Phillip'
            print("REINOS: GOT IT!")

        title_value = re.sub("broad institute of harvard and massachusetts institute of technology", "",title_value)
        title_value = re.sub("broad institute of harvard", "",title_value)
        title_value = re.sub("massachusetts institute of technology", "",title_value)
        title_value = re.sub("harvard|\d+|cambridge|massachusetts|\,|hospital|united states of america|department of|boston|huntington|avenue|kresge| ma | usa|brigham and women\’s|medical school","",title_value)
        title_value = re.sub("school of [^,]+","",title_value)
        title_value = re.sub(" +"," ",title_value)
        print("REINOS: title_value: " + title_value)
        if len(nameparts) > 1 :
            middle = nameparts[1]
        url = "{base_url}surname={last}&givenname={first}&school={school}&title={title}".format(
            base_url = base_url, last = enc(last), first = enc(first), school = enc(school_value), title = enc(title_value))

        if middle:
            url+= "&middlename=" + enc(middle)
        dept_value = bulklib.findit("department of ([\w ]+)",afftexts)
        # skip departments for now.
        if dept_value :
            print("REINOS: got a department!" + dept_value)
            url+= "&department=" + enc(dept_value)
        print("REINOS: building author huid lookup url : " + url)
        print("REINOS: fetching this url...")
        #json_string = urllib.request.urlopen(url).read().decode('utf-8')
        json_string = urllib.request.urlopen(url).read().decode('ISO-8859-1')
        print("REINOS: got this json string: " + json_string)
        json_authors = json.loads(json_string)['choices']
        AR['json_url']= url
        AR['ldap_authors']=json_authors
        AR['pmcid']=article['pmcid']
        author['match_count'] = len(json_authors)
        print("REINOS: got this many json authors: " + str(len(json_authors)))
        best_json_author = get_best_json_author(json_authors)
        if best_json_author :
            print("REINOS: got a best author: " + str(best_json_author['confidence']))
            AR['best_match_author'] = best_json_author
            author['authority'] = best_json_author['authority']
            article['harvard_authors'].append(author)
            article['found_any_harvard_auths']=True
            try:
                for school in best_json_author['schools'] :
                    add_ldap_school(article,author,school)
            except:
                pass

    if article['found_any_harvard_auths'] :
        article['found_all_harvard_auths']=True # tentative
        for author in article['authors'] :
            if author['has_harvard_affstring'] and author['authority'] == UNAFFILIATED :
                article['found_all_harvard_auths']=False
                return

def get_best_json_author(json_authors) :
    bja = None
    tie = False
    for ja in json_authors :
        if float(ja['confidence']) < .34 :
            continue
        if bja == None or bja['confidence'] < ja['confidence'] :
            bja=ja
            tie = False
        elif bja['confidence'] == ja['confidence'] :
            tie = True
    if tie :
        return None
    else :
        return bja

def add_pmc_school(article,author,school,afftext) :
    # these are schools based on pmc affiliation text.
    print("REINOS: adding pmc school: " + school)
    article['pmc_schools'].add(school)
    author['pmc_schools'].add(school)
    author['afftexts'].append(afftext)
    author['has_harvard_affstring']=True

def add_ldap_school(article,author,school):
    # these are schools based on ldap lookup.
    article['ldap_schools'].add(school)
    author['ldap_schools'].add(school)

def assign_article_schools(article,fas_depts) :
    # primitive assignment of article to schools based on author affiliation string.
    for author in article['authors'] :
        author['pmc_schools']=set()
        author['afftexts']=[]
        for aff in author['affs'] :
            afftext=aff['text'].lower()
            afftext = re.sub("harvard\.edu","",afftext) # remove emails as basis for affiliation.
            #author['afftext']+=afftext
            print("REINOS_X: afftext: " + afftext)
            if re.match(".*(harvard medical school|harvard university school of medicine|beth israel deaconess medical center|brigham and women\’s hospital|massachusetts general hospital).*",afftext) :
                add_pmc_school(article,author,'HMS',afftext)
            elif re.match(".*harvard school of public health.*",afftext) :
                # does changing HSPH to SPH matter here?
                #add_pmc_school(article,author,'HSPH',afftext)
                add_pmc_school(article,author,'SPH',afftext)
            elif re.match(".*harvard graduate school of education.*",afftext) :
                add_pmc_school(article,author,'GSE',afftext)
            elif re.match(".*harvard university.*",afftext) :
                print("FAS afftext: " + afftext)
                print(author)
                for dept in fas_depts :
                    clean_dept    = dept.lower()
                    clean_afftext = re.sub("\&","and",afftext)
                    if re.match(".*"+ clean_dept +".*",clean_afftext) :
                        print("REINOS: FAS DEPT: " + dept)
                        article['pmc_depts'].add(dept)
                        break
                if len(article['pmc_depts']) > 0 :
                    add_pmc_school(article,author,'FAS',afftext)
                else :
                    # no idea what school to attach this author to. maybe ldap will tell us...
                    add_pmc_school(article,author,'',afftext)
            elif re.match(".*harvard.*",afftext) :
                add_pmc_school(article,author,'',afftext)
            else :
                print("REINOS_X: NO HARVARD AFF MATCH!")
    print("pmc schools:")
    print(article['pmc_schools'])
    print("pmc departments:")
    print(article['pmc_depts'])


def format_first(author,initialize = False) :
    '''Return authors first + middle name or first/middle initials.'''
    if initialize:
        first = ""
        if 'first' in author:
            return " ".join(part[0:1] + "." for part in author['first'].split(" ") if part[0:1])
    else:
        return author['first']

def build_citation(article):
    # build_citation_new
    citation=""
    i = 1
    et_al = len(article['authors']) >= 11
    for author in article['authors'] :
        # if 11 or more authors, limit to 7 with "et al"
        if len(article['authors']) >= 11 and i > 7 :
            citation+= "et al."
            break
        if author.get('first', None) == None :
            # it's not a person, but a consortium or something -- no first name.
            citation += author['last']
        elif i == 1 :
            citation += "{}, {}, ".format(author['last'], format_first(author,et_al))
        else :
            if i == len(article['authors']) :
                if citation[-4] == ' ' :
                    citation = re.sub(", $","., ",citation)
                citation += "and "
            citation+= "{} {}, ".format(format_first(author,et_al), author['last'])
        i+=1
    citation += ". {}. “{}.” ".format(article['date'], article['title'])

    citation += article['journal'] + " "
    citation = re.sub(", \.", ".", citation)
    citation = re.sub("\.\.",".",citation)
    citation = re.sub("\s+"," ",citation)
    citation = re.sub("\?\.","?",citation)
    if 'volume' in article :
        citation += article['volume']
        if 'issue' not in article :
            # mimic crossref logic.
            article['issue']='1'

    if 'issue' in article:
        citation += " (" + article['issue'] + ")"

    if 'fpage' in article:
        fpage  = article['fpage']
        pages = ': ' + fpage
        if 'lpage' in article:
            lpage  = article['lpage']
            if lpage != None and fpage != lpage:
                pages = ': ' + fpage + '-' + lpage
        citation += pages
    else :
        pages = ""

    citation = citation.lstrip()

    # question: how do elocation ids play with dois?
    if 'elocation-id' in article :
        print("REINOS: HOT DOG! sticking elocation-id into citation")
        # Chicago Style dictates a space after the colon if there is an issue number in parentheses, NO space if there is no issue number
        # -- Emily Andersen
        citation += ":"
        if 'issue' in article :
           citation += ' '
        citation += article['elocation-id']



    if 'doi' in article:
        citation += ". doi:" + article['doi']
        citation += ". http://dx.doi.org/" + article['doi']


    citation = re.sub(" +"," ",citation)
    citation = re.sub(" ,",",",citation)
    citation += '.'

    print("REINOS: CITATION: " + citation)

    # test a few citations vs. slightly tweaked dash crossref api output.
    doi2citation={}
    doi2citation['10.2337/dc11-2420']="Beer, N. L., K. K. Osbak, M. van de Bunt, N. D. Tribble, A. M. Steele, K. J. Wensley, E. L. Edghill, et al. 2012. “Insights Into the Pathogenicity of Rare Missense GCK Variants From the Identification and Functional Characterization of Compound Heterozygous and Double Mutations Inherited in Cis.” Diabetes Care 35 (7): 1482-1484. doi:10.2337/dc11-2420. http://dx.doi.org/10.2337/dc11-2420."
    doi2citation['10.2337/dc12-0073']="Cozma, A. I., J. L. Sievenpiper, R. J. de Souza, L. Chiavaroli, V. Ha, D. D. Wang, A. Mirrahimi, et al. 2012. “Effect of Fructose on Glycemic Control in Diabetes: A systematic review and meta-analysis of controlled feeding trials.” Diabetes Care 35 (7): 1611-1620. doi:10.2337/dc12-0073. http://dx.doi.org/10.2337/dc12-0073."
    doi2citation['10.1186/2045-5380-3-12']="Huys, Quentin JM, Diego A Pizzagalli, Ryan Bogdan, and Peter Dayan. 2013. “Mapping anhedonia onto reinforcement learning: a behavioural meta-analysis.” Biology of Mood & Anxiety Disorders 3 (1): 12. doi:10.1186/2045-5380-3-12. http://dx.doi.org/10.1186/2045-5380-3-12."
    doi2citation['10.2337/db11-0134']="Azzi, J., R. F. Moore, W. Elyaman, M. Mounayar, N. El Haddad, S. Yang, M. Jurewicz, et al. 2012. “The Novel Therapeutic Effect of Phosphoinositide 3-Kinase-γ Inhibitor AS605240 in Autoimmune Diabetes.” Diabetes 61 (6): 1509-1518. doi:10.2337/db11-0134. http://dx.doi.org/10.2337/db11-0134."
    doi2citation['10.2337/db11-1296']="Haiman, C. A., M. D. Fesinmeyer, K. L. Spencer, P. Bůžková, V. S. Voruganti, P. Wan, J. Haessler, et al. 2012. “Consistent Directions of Effect for Established Type 2 Diabetes Risk Variants Across Populations: The Population Architecture using Genomics and Epidemiology (PAGE) Consortium.” Diabetes 61 (6): 1642-1647. doi:10.2337/db11-1296. http://dx.doi.org/10.2337/db11-1296."
    doi2citation['10.1371/journal.pone.0067405']="Palmsten, Kristin, Krista F. Huybrechts, Helen Mogun, Mary K. Kowal, Paige L. Williams, Karin B. Michels, Soko Setoguchi, and Sonia Hernández-Díaz. 2013. “Harnessing the Medicaid Analytic eXtract (MAX) to Evaluate Medications in Pregnancy: Design Considerations.” PLoS ONE 8 (6): e67405. doi:10.1371/journal.pone.0067405. http://dx.doi.org/10.1371/journal.pone.0067405."
    doi2citation['10.1111/nyas.12031']="Weir, Gordon C., and Susan Bonner-Weir. 2013. “Islet β cell mass in diabetes and how it relates to function, birth, and death.” Annals of the New York Academy of Sciences 1281 (1): 92-105. doi:10.1111/nyas.12031. http://dx.doi.org/10.1111/nyas.12031."

    doi2citation['10.4081/hi.2011.e14']="Huffman, Jeff C., Carol A. Mastromauro, Julia K. Boehm, Rita Seabrook, Gregory L. Fricchione, John W. Denninger, and Sonja Lyubomirsky. 2011. “Development of a positive psychology intervention for patients with acute cardiovascular disease.” Heart International 6 (2): e14. doi:10.4081/hi.2011.e14. http://dx.doi.org/10.4081/hi.2011.e14."

    if 'doi' in article and article['doi'] in doi2citation:
        c = doi2citation[article['doi']]
        if citation == c :
            print("REINOS_SUCCESS")
        else :
            print("REINOS_FAILURE")
            print("REINOS: Shouldbe: " +c)
            exit()


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

    # stuff keywords into subject as well if available. (Emily Anderson request)
    for kwd_group_node in findall(article_node,'kwd-group') :
        for kwd_node in findall(kwd_group_node,'kwd') :
            if kwd_node.text not in subjects and kwd_node.text != None :
                # remove parenthesized numeric subject codes.
                keyword = re.sub("^\(\d+\.\d+\)","",kwd_node.text)
                subjects.append(keyword)

    return subjects


def extract_text(node) :
    # get concatenated text string for all subnodes if any.
    text=""
    for t in abstract_node.itertext() :
        text+=t
    return text


def catnode(node):
    '''Concatenate text for all subnotes, with whitespace cleanup.'''
    cattext=""
    for text in node.itertext() :
        text = re.sub("\s+"," ",text)
        cattext+=text
    return cattext.strip()


def extract_abstract(article_node):
    # complex because in a bunch of subnodes. this method is crude.
    abstract_nodes =findall(article_node,'abstract')
    abstract_node = None

    if len(abstract_nodes) == 1 :
        # just one abstract so use it.
        abstract_node = abstract_nodes[0]
    elif len(abstract_nodes) >= 2 :
        # mulitiple abstracts: take the "normal," unqualified abstract (not the precis, toc etc.)
        for node in abstract_nodes:
            if 'abstract-type' in node.attrib :
                print("REINOS: dodging abstract: " + node.attrib['abstract-type'])
            else :
                print("REINOS: assigning unqualified abstract.")
                abstract_node = node

    abstract = ""
    regex = re.compile(
        r"(" + r"|".join(
            (r"Author Summary", r"Background", r"Case presentation", r"Conclusion", r"Conclusions", r"Conclusions\/Significance",
             r"Design", "eLife digest", "Findings", "IMPORTANCE", "Introduction",
             r"Main Outcome Measures?", r"Main Results", r"Methods(?: (?:&|and) (?:(?:(?:Principal )?Findings)|Results))?", r"Methods\/Findings",
             r"Objectives?", r"Participants", r"Rationale", r"Research Design & Methods", r"Results", "Setting(?: and Participants)?",)
        ) + r")$") # end regex = assignment

    if abstract_node is not None:
        for text in abstract_node.itertext() :
            abstract += text

            if regex.match(text):
                # a bit hacky. should really add colon after "title" elements in abstact.
                abstract+=":"
    abstract = re.sub("\s+"," ",abstract)

    return abstract.strip()


def already_in_dash(article,dash_dois,dash_titles,dash_pmcids) :
    # first cut at duplicate detection.
    found = False
    if 'doi' in article and article['doi'] in dash_dois :
        found = True
    if article['pmcid'] in dash_pmcids :
        found = True
    if article['title'] in dash_titles :
        found = True
    return found


def get_target_collection_dir(article):
    '''build the collection directory name based on LDAP schools with fallback to PMC schools (converted to dash school naming convention).'''
    schools = []
    for ldap_school in sorted(article['ldap_schools']) :
        if ldap_school in LDAP2DASH_SCHOOL :
            schools.append(LDAP2DASH_SCHOOL[ldap_school])
        else:
            print("REINOS: hey! no DASH school for LDAP school: " + ldap_school)

    if not schools :
        print("No LDAP schools... let's try PMC schools")
        for pmc_school in sorted(article['pmc_schools']) :
            schools.append(pmc_school)

    collection = "_".join(schools)
    return collection


def download_files(article, batch):
    # TODO: replace with FTP
    # TODO: handle non-pdf articles
    # TODO: can there be multiple files?
    pdf_url = "http://www.ncbi.nlm.nih.gov/pmc/articles/PMC" + article['pmcid'] + "/pdf/"
    article['hasversion']=pdf_url
    file = {'url': pdf_url,
            'name': article['pmcid']+'.pdf'}

    cachepath = DATA_DIR + "/batch/"+batch+"/articles"
    if not os.path.exists(cachepath):
        os.mkdir(cachepath)
    file['cachepath']  =  cachepath+ "/" +file['name']
    errorpath = file['cachepath'] + ".error"
    if ( os.path.exists(file['cachepath']) or os.path.exists(errorpath) ):
        print("Article file in cache...");
    else:
        print("Downloading " + file['url'] + " to " + file['cachepath'])
        request = urllib.request.Request(file['url'])
        request.add_header('User-Agent','Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.53 Safari/534.3')
        try:
            f = urllib.request.urlopen(request)
            with open(file['cachepath'], "wb") as local_file:
                local_file.write(f.read())

        except urllib.error.URLError as e:
            with open(errorpath, "w") as local_file:
                local_file.write('Error getting url:\n'+file['url'])
                local_file.write('Code: ' + str(e.code))
                local_file.write('Read: ' + str(e.read))

        time.sleep(random.randint(3, 6))
    if os.path.exists(file['cachepath']):
        article['files'].append(file)


def write_output(batch,batch_out_dir,article,article_number):
    target_collection = get_target_collection_dir(article)
    collection_out_dir = batch_out_dir + "/" + target_collection
    article_out_dir= collection_out_dir +"/"+str(article_number)

    for d in [batch_out_dir, collection_out_dir, article_out_dir]:
        if not os.path.exists(d):
            os.mkdir(d)

    bulklib.write_dublin_core_meta(article,article_out_dir,batch)
    bulklib.write_dash_meta(article,article_out_dir)
    bulklib.write_contents_file(article,article_out_dir)

    for file in article['files'] :
        shutil.copyfile(file['cachepath'], article_out_dir + "/" + file['name'])
    shutil.copyfile(DATA_DIR+"/licenses/" + article['license'] + '/license.txt', article_out_dir+'/license.txt')


def prep_batch_out_dir(batch_out_dir):
    if os.path.exists(batch_out_dir):
        print("Deleting existing batch_out_dir: " + batch_out_dir)
        shutil.rmtree(batch_out_dir)
    print("Creating batch_out_dir: " + batch_out_dir)
    os.mkdir(batch_out_dir)

main()
