import argparse
import os
import io
import sys
from urllib import parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import boto3
import botocore
import magic
import requests
import shutil
import gzip
import tarfile
import json
import pickle
import subprocess
import S3
import csv
import time
import uuid
import lmdb
from tqdm import tqdm

map_size = 100 * 1024 * 1024 * 1024 

class Harverster(object):
    """
    What:
    - Harvester for article set (list of DOI, PMID, PMC ID or basic metadata provided in a csv file, e.g. CORD-19 csv metadata file) 
      with robust parallel PDF download
    - Perform some metadata enrichment/agregation via biblio-glutton/CrossRef API and output consolidated metadata in a json file 
    - Perform Grobid full processing of PDF (including bibliographical reference consolidation and OA access resolution)

    Optionally: 
    - generate thumbnails for article (first page) 
    - load stuff on S3 instead of local file
    - generate json PDF annotation (with coordinates) for inline reference markers and bibliographical references 

    Usage: see the Readme.md file
    """

    def __init__(self, config_path='./config.json', thumbnail=False, sample=None, dump_metadata=False, annotation=False, only_download=False):
        # boolean indicating if we only want to download the raw files without structuring them into XML
        self.only_download = only_download

        self.config = None   
        self._load_config(config_path)

        # here are store stable resources like identifier mapping and archive download mapping
        self.resource_path = "./resources"

        # the file where all the metadata are stored
        self.dump_file = dump_metadata

        # boolean indicating if we want to generate thumbnails of front page of PDF 
        self.thumbnail = thumbnail

        self.annotation = annotation

        # if a sample value is provided, indicate that we only harvest the indicated number of PDF
        self.sample = sample

        self.s3 = None
        if self.config["bucket_name"] is not None and len(self.config["bucket_name"]) is not 0:
            self.s3 = S3.S3(self.config)

        # in case we use a local folder filled with Elsevier COVID-19 Open Access PDF from their ftp server
        self.elsevier_oa_map = None
        self._init_local_file_map()

        # the following lmdb map gives for every PMC ID where to download the archive file containing NLM and PDF files
        self.env_pmc_oa = None

        # standard lmdb environment for storing biblio entries by uuid
        self.env_entries = None

        # lmdb environment for storing mapping between sha/doi/pmcid and uuid
        self.env_uuid = None

        self._init_lmdb()

        self.dump_file_name = "consolidated_metadata.json"

    def _load_config(self, path='./config.json'):
        """
        Load the json configuration 
        """
        config_json = open(path).read()
        self.config = json.loads(config_json)

        # test if GROBID is up and running, except if we just want to download raw files
        if not self.only_download:
            the_url = _grobid_url(self.config['grobid_base'], self.config['grobid_port'])
            the_url += "isalive"
            r = requests.get(the_url)
            if r.status_code != 200:
                print('GROBID server does not appear up and running ' + str(r.status_code))
            else:
                print("GROBID server is up and running")

    def _init_local_file_map(self):
        # build the local file map, if any, for the Elsevier COVID-19 OA set
        # TBD: this might better go to its own LMDB map than staying in memory like this!
        if self.config["cord19_elsevier_pdf_path"] is not None and len(self.config["cord19_elsevier_pdf_path"])>0 and self.elsevier_oa_map is None:
            # init map
            self.elsevier_oa_map = {}
            if os.path.isfile(os.path.join(self.resource_path, "elsevier_covid_map_20_09_2020.csv.gz")):
                with gzip.open(os.path.join(self.resource_path, "elsevier_covid_map_20_09_2020.csv.gz"), mode="rt") as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        if row["doi"] is not None and len(row["doi"])>0:
                            self.elsevier_oa_map[row["doi"].lower()] = row["pdf"]
                        if row["pii"] is not None and len(row["pii"])>0:    
                            self.elsevier_oa_map[row["pii"]] = row["pdf"]

    def _init_lmdb(self):
        # create the data path if it does not exist 
        if not os.path.isdir(self.config["data_path"]):
            try:  
                os.makedirs(self.config["data_path"])
            except OSError:  
                print ("Creation of the directory %s failed" % self.config["data_path"])
            else:  
                print ("Successfully created the directory %s" % self.config["data_path"])

        # open in write mode
        envFilePath = os.path.join(self.config["data_path"], 'entries')
        self.env_entries = lmdb.open(envFilePath, map_size=map_size)

        envFilePath = os.path.join(self.config["data_path"], 'uuid')
        self.env_uuid = lmdb.open(envFilePath, map_size=map_size)

        # build the PMC map information, in particular for downloading the archive file containing the PDF and XML 
        # files (PDF not always present)
        resource_file = os.path.join(self.resource_path, "oa_file_list.txt")
        # TBD: if the file is not present we should download it at ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt
        if not os.path.isfile(resource_file):
            # TBD: if the file is not present we should download it at ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt
            url = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt"
            print("Downloading PMC resource file:", url)
            _download(url, resource_file)

        envFilePath = os.path.join(self.resource_path, 'pmc_oa')
        if os.path.isfile(resource_file) and not os.path.isdir(envFilePath):
            # open in write mode
            self.env_pmc_oa = lmdb.open(envFilePath, map_size=map_size)
            txn = self.env_pmc_oa.begin(write=True)

            nb_lines = 0
            # get number of line in the file
            with open(resource_file, "r") as fp:
                for line in fp:
                    nb_lines += 1

            # fill this lmdb map
            print("building PMC resource map")
            with open(resource_file, "r") as fp:
                count = 0
                for line in tqdm(fp, total=nb_lines):
                    if count == 0:
                        #skip first line which is just a time stamp
                        count += 1
                        continue
                    row = line.split('\t')
                    subpath = row[0]
                    pmcid = row[2]
                    # pmid is optional
                    pmid= row[3]
                    license = row[4]
                    localInfo = {}
                    localInfo["subpath"] = subpath
                    localInfo["pmid"] = pmid
                    localInfo["license"] = license
                    txn.put(pmcid.encode(encoding='UTF-8'), _serialize_pickle(localInfo)) 
                    count += 1
            txn.commit()               
            self.env_pmc_oa.close()

        # open in read mode only
        self.env_pmc_oa = lmdb.open(envFilePath, readonly=True, lock=False)

    def unpaywalling_doi(self, doi):
        """
        Check the Open Access availability of the DOI via Unpaywall, return the best download URL or None otherwise.
        We need to use the Unpaywall API to get fresh information, because biblio-glutton is based on the 
        Unpaywall dataset dump which has a 7-months gap.
        """
        response = requests.get(self.config["unpaywall_base"] + doi, params={'email': self.config["unpaywall_email"]}).json()
        if response['best_oa_location'] and response['best_oa_location']['url_for_pdf']:
            return response['best_oa_location']['url_for_pdf']
        elif response['best_oa_location']['url'].startswith(self.config['pmc_base_web']):
            return response['best_oa_location']['url']+"/pdf/"
        # we have a look at the other "oa_locations", which might have a `url_for_pdf` ('best_oa_location' has not always a 
        # `url_for_pdf`, for example for Elsevier OA articles)
        for other_oa_location in response['oa_locations']:
            # for a PMC file, we can concatenate /pdf/ to the base, eg https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7029158/pdf/
            # but the downloader will have to use a good User-Agent and follow redirection
            if other_oa_location['url'].startswith(self.config['pmc_base_web']):
                return other_oa_location['url']+"/pdf/"
            if other_oa_location['url_for_pdf']:
                return other_oa_location['url_for_pdf']
        return None

    def elsevier_oa_check(self, doi=None, pii=None):
        # this is a list of OA articles from Elsevier, e.g. COVID papers, if successful it will return the path
        # to the local PDF corresponding to this article
        # we can download these pdf set from their dedicated ftp, and make them available locally for this dataset builder
        # note: also direct download link for pdf - but maybe some risks to be blocked?
        # https://www.sciencedirect.com/science/article/pii/S0924857920300674/pdfft?isDTMRedir=true&download=true
        # their API is not even up to date: https://api.elsevier.com/content/article/pii/S0924857920300674
        # still described as closed access
        if self.elsevier_oa_map is None:
            return None

        if doi is None and pii is None:
            return None

        if self.config["cord19_elsevier_pdf_path"] is None or len(self.config["cord19_elsevier_pdf_path"]) == 0:
            return None

        '''
        if doi is not None:
            print(doi)
            if doi.lower() in self.elsevier_oa_map:
                print(self.elsevier_oa_map[doi.lower()])
        '''
        if doi is not None and doi.lower() in self.elsevier_oa_map:
            return os.path.join(self.config["cord19_elsevier_pdf_path"],self.elsevier_oa_map[doi.lower()])

        if pii is not None and pii in self.elsevier_oa_map:
            return os.path.join(self.config["cord19_elsevier_pdf_path"],self.elsevier_oa_map[pii])

    def pmc_oa_check(self, pmcid):
        try:
            with self.env_pmc_oa.begin() as txn:
                pmc_info_object = txn.get(pmcid.encode(encoding='UTF-8'))
                if pmc_info_object:
                    try:
                        pmc_info = _deserialize_pickle(pmc_info_object)
                    except:
                        print("omg _deserialize_pickle failed?")
                    if "license" in pmc_info:
                        license = pmc_info["license"]
                        license = license.replace("\n","")
                    else:
                        license = ""
                    if "subpath" in pmc_info:
                        subpath = pmc_info["subpath"];
                        return os.path.join(self.config["pmc_base_ftp"],subpath), license
        except lmdb.Error:
            print("lmdb pmc os look-up failed")
        return None, None

    def biblio_glutton_lookup(self, doi=None, pmcid=None, pmid=None, istex_id=None, istex_ark=None):
        """
        Lookup on biblio_glutton with the provided strong identifiers, return the full agregated biblio_glutton record
        """
        biblio_glutton_url = _biblio_glutton_url(self.config["biblio_glutton_base"], self.config["biblio_glutton_port"])
        success = False
        jsonResult = None

        if doi is not None and len(doi)>0:
            response = requests.get(biblio_glutton_url, params={'doi': doi})
            success = (response.status_code == 200)
            if success:
                jsonResult = response.json()

        if not success and pmid is not None and len(pmid)>0:
            response = requests.get(biblio_glutton_url + "pmid=" + pmid)
            success = (response.status_code == 200)
            if success:
                jsonResult = response.json()     

        if not success and pmcid is not None and len(pmcid)>0:
            response = requests.get(biblio_glutton_url + "pmc=" + pmcid)  
            success = (response.status_code == 200)
            if success:
                jsonResult = response.json()

        if not success and istex_id is not None and len(istex_id)>0:
            response = requests.get(biblio_glutton_url + "istexid=" + istex_id)
            success = (response.status_code == 200)
            if success:
                jsonResult = response.json()
        
        if not success and doi is not None and len(doi)>0:
            # let's call crossref as fallback for the X-months gap
            # https://api.crossref.org/works/10.1037/0003-066X.59.1.29
            user_agent = {'User-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0 (mailto:' 
                + self.config['crossref_email'] + ')'} 
            response = requests.get(self.config['crossref_base']+"/works/"+doi, headers=user_agent)
            if response.status_code == 200:
                jsonResult = response.json()['message']
                # filter out references and re-set doi, in case there are obtained via crossref
                if "reference" in jsonResult:
                    del jsonResult["reference"]
            else:
                success = False
                jsonResult = None
        
        return jsonResult

    def reset(self, dump_file=False):
        """
        Remove the local files and lmdb keeping track of the state of advancement of the harvesting and
        of the failed entries
        """
        # close environments
        self.env_entries.close()
        self.env_uuid.close()

        # clean any possibly remaining tmp files
        for f in os.listdir(self.config["data_path"]):
            if f.endswith(".pdf") or f.endswith(".png") or f.endswith(".nxml") or f.endswith(".xml") or f.endswith(".tar.gz") or f.endswith(".json"):
                os.remove(os.path.join(self.config["data_path"], f))

            # clean any existing data files, except 
            path = os.path.join(self.config["data_path"], f)
            if os.path.isdir(path):
                try:
                    shutil.rmtree(path)
                except OSError as e:
                    print("Error: %s - %s." % (e.filename, e.strerror))

        # clean the metadata file if present
        if self.dump_file: 
            if os.path.isfile(self.dump_file_name):
                os.remove(self.dump_file_name)

        # re-init the environments
        self._init_lmdb()

    def dump_metadata(self):
        if self.dump_file_name is None:
            self.dump_file_name = "consolidated_metadata.json"

        # init lmdb transactions
        txn = self.env_entries.begin(write=True)
        
        nb_total = txn.stat()['entries']
        print("number of harvested entries:", nb_total)

        with open(self.dump_file_name,'w') as file_out:
            # iterate over lmdb
            cursor = txn.cursor()
            for key, value in cursor:
                if txn.get(key) is None:
                    continue
                local_entry = _deserialize_pickle(txn.get(key))
                file_out.write(json.dumps(local_entry, sort_keys=True))
                file_out.write("\n")

        # we need to upload to S3 the consolidated metadata file, if S3 has been set
        if self.s3 is not None:
            if os.path.isfile(self.dump_file_name):
                self.s3.upload_file_to_s3(self.dump_file_name, ".", storage_class='ONEZONE_IA')


    def run_grobid(self, pdf_file, output=None, annotation_output=None):
        # normal fulltext TEI file
        print("run grobid:", pdf_file, output)
        if output is not None:
            files = {
                'input': (
                    pdf_file,
                    open(pdf_file, 'rb'),
                    'application/pdf',
                    {'Expires': '0'}
                )
            }
            
            the_url = _grobid_url(self.config['grobid_base'], self.config['grobid_port'])
            the_url += "processFulltextDocument"

            # set the GROBID parameters
            the_data = {}
            the_data['generateIDs'] = '1'
            the_data['consolidateHeader'] = '1'
            the_data['consolidateCitations'] = '1'   
            the_data['includeRawCitations'] = '1'

            r = requests.request(
                "POST",
                the_url,
                headers={'Accept': 'application/xml'},
                files=files,
                data=the_data,
                timeout=60
            )

            status = r.status_code
            if status == 503:
                time.sleep(self.config['sleep_time'])
                return self.process_pdf(pdf_file, output, None)
            elif status != 200:
                print('Processing failed with error ' + str(status))
            else:
                # writing TEI file
                try:
                    with io.open(output,'w',encoding='utf8') as tei_file:
                        tei_file.write(r.text)
                except OSError:  
                   print ("Writing resulting TEI XML file %s failed" % output)

        # reference annotation file
        if annotation_output is not None:
            # we have to re-open the PDF file
            files = {
                'input': (
                    pdf_file,
                    open(pdf_file, 'rb'),
                    'application/pdf',
                    {'Expires': '0'}
                )
            }

            the_url = _grobid_url(self.config['grobid_base'], self.config['grobid_port'])
            the_url += "referenceAnnotations"

            # set the GROBID parameters
            the_data = {}
            the_data['consolidateCitations'] = '1'   

            r = requests.request(
                "POST",
                the_url,
                headers={'Accept': 'application/json'},
                files=files,
                data=the_data,
                timeout=60
            )

            status = r.status_code
            if status == 503:
                time.sleep(self.config['sleep_time'])
                return self.process_pdf(pdf_file, None, annotation_output)
            elif status != 200:
                print('Processing failed with error ' + str(status))
            else:
                # writing TEI file
                try:
                    with io.open(annotation_output,'w',encoding='utf8') as json_file:
                        json_file.write(r.text)
                except OSError:  
                   print ("Writing resulting JSON file %s failed" % annotation_output)

    def harvest_dois(self, dois_file):
        with open(dois_file, 'rt') as fp:
            line_count = 0 # total count of articles
            i = 0 # counter for article per batch
            identifiers = []
            dois = []  
            for count, line in enumerate(fp):
                if len(line.strip()) == 0:
                    continue

                if i == self.config["batch_size"]:
                    with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                        # branch to the right entry processor, depending on the input csv 
                        executor.map(self.processEntryDOI, identifiers, dois)
                    # reinit
                    i = 0
                    identifiers = []
                    dois = []

                the_doi = line.strip()
                the_doi = _clean_doi(the_doi)
                # check if the entry has already been processed
                if self.getUUIDByStrongIdentifier(the_doi) is not None:
                    line_count += 1
                    continue

                # we need a new identifier
                identifier = str(uuid.uuid4())
                identifiers.append(identifier)
                dois.append(the_doi)
    
                line_count += 1
                i += 1
            
            # we need to process the last incomplete batch, if not empty
            if len(identifiers) > 0:
                with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                    # branch to the right entry processor, depending on the input csv 
                    executor.map(self.processEntryDOI, identifiers, dois)

            print("processed", str(line_count), "articles")

    def harvest_cord19(self, metadata_csv_file):
        # cord_uid,sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,Microsoft Academic Paper ID,
        # WHO #Covidence,has_full_text,full_text_file,url
        with open(metadata_csv_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0 # total count of articles
            i = 0 # counter for article per batch
            identifiers = []
            rows = []
            for row in csv_reader:

                if i == self.config["batch_size"]:
                    with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                        # branch to the right entry processor, depending on the input csv 
                        executor.map(self.processEntryCord19, identifiers, rows)
                    # reinit
                    i = 0
                    identifiers = []
                    rows = []

                # check if the entry has already been processed
                # we can use from 27.03.2020 update the cord_uid as identifier, and keep doi of course as fallback
                # we don't use the sha as identifier, just keep it in the metadata
                if row["cord_uid"] and len(row["cord_uid"])>0:
                    # in the current version, there is always a cord_uid normally
                    if self.getUUIDByStrongIdentifier(row["cord_uid"]) is not None:
                        line_count += 1
                        continue
                elif row["doi"] and len(row["doi"])>0:
                    if self.getUUIDByStrongIdentifier(row["doi"]) is not None:
                        line_count += 1
                        continue

                # we use cord_uid as identifier
                identifier = row["cord_uid"]
                identifiers.append(identifier)
                rows.append(row)
    
                line_count += 1
                i += 1
            
            # we need to process the last incomplete batch, if not empty
            if len(identifiers) >0:
                with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                    # branch to the right entry processor, depending on the input csv 
                    executor.map(self.processEntryCord19, identifiers, rows)

            print("processed", str(line_count), "articles from CORD-19")

    def harvest_pmids(self, pmids_file):
        with open(pmids_file, 'rt') as fp:
            line_count = 0 # total count of articles
            i = 0 # counter for article per batch
            identifiers = []
            pmids = []  
            for count, line in enumerate(fp):
                if len(line.strip()) == 0:
                    continue

                if i == self.config["batch_size"]:
                    with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                        executor.map(self.processEntryPMID, identifiers, pmids)
                    # reinit
                    i = 0
                    identifiers = []
                    pmids = []

                the_pmid = line.strip()
                # check if the entry has already been processed
                if self.getUUIDByStrongIdentifier(the_pmid) is not None:
                    line_count += 1
                    continue

                # we need a new identifier
                identifier = str(uuid.uuid4())
                identifiers.append(identifier)
                pmids.append(the_pmid)
    
                line_count += 1
                i += 1
            
            # we need to process the last incomplete batch, if not empty
            if len(identifiers) > 0:
                with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                    executor.map(self.processEntryPMID, identifiers, pmids)

            print("processed", str(line_count), "article PMID")

    def harvest_pmcids(self, pmcids_file):
        with open(pmcids_file, 'rt') as fp:
            line_count = 0 # total count of articles
            i = 0 # counter for article per batch
            identifiers = []
            pmcids = []  
            for count, line in enumerate(fp):
                if len(line.strip()) == 0:
                    continue

                if i == self.config["batch_size"]:
                    with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                        executor.map(self.processEntryPMCID, identifiers, pmids)
                    # reinit
                    i = 0
                    identifiers = []
                    pmcids = []

                the_pmcid = line.strip()
                # check if the entry has already been processed
                if self.getUUIDByStrongIdentifier(the_pmcid) is not None:
                    line_count += 1
                    continue

                # we need a new identifier
                identifier = str(uuid.uuid4())
                identifiers.append(identifier)
                pmcids.append(the_pmcid)
    
                line_count += 1
                i += 1
            
            # we need to process the last incomplete batch, if not empty
            if len(identifiers) > 0:
                with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                    executor.map(self.processEntryPMCID, identifiers, pmcids)

            print("processed", str(line_count), "article PMC ID")

    def processEntryDOI(self, identifier, doi):
        localJson = self.biblio_glutton_lookup(doi=doi, pmcid=None, pmid=None, istex_id=None, istex_ark=None)
        if localJson is None:
            localJson = {}
            localJson['DOI'] = doi
        localJson["id"] = identifier

        print("processing", localJson['DOI'], "as", identifier)

        localJson = _initProcessStateInformation(localJson)
        self.updateIdentifierMap(localJson)
        self.processTask(localJson)

    def processEntryPMID(self, identifier, pmid):
        localJson = self.biblio_glutton_lookup(doi=None, pmcid=None, pmid=pmid, istex_id=None, istex_ark=None)
        if localJson is None:
            localJson = {}
            localJson['pmid'] = pmid
        localJson["id"] = identifier

        print("processing", localJson['pmid'], "as", identifier)

        localJson = _initProcessStateInformation(localJson)
        self.updateIdentifierMap(localJson)
        self.processTask(localJson)

    def processEntryPMCID(self, identifier, pmcid):
        localJson = self.biblio_glutton_lookup(doi=None, pmcid=pmcid, pmid=None, istex_id=None, istex_ark=None)
        if localJson is None:
            localJson = {}
            localJson['pmcid'] = pmcid
        localJson["id"] = identifier

        print("processing", localJson['pmcid'], "as", identifier)

        localJson = _initProcessStateInformation(localJson)
        self.updateIdentifierMap(localJson)
        self.processTask(localJson)
            
    def processEntryCord19(self, identifier, row):
        # cord_uid,sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,Microsoft Academic Paper ID,
        # WHO #Covidence,has_full_text,full_text_file,url  
        try:    
            localJson = self.biblio_glutton_lookup(doi=_clean_doi(row["doi"]), pmcid=row["pmcid"], pmid=row["pubmed_id"], istex_id=None, istex_ark=None)
        except:
            print("biblio-glutton call fails")
            localJson = None

        if localJson is None:
            localJson = {}
            localJson['title'] = row["title"]
            localJson['year']= row["publish_time"]
        
        localJson["id"] = identifier
        # add the CORD-19 sha, though it won't be used
        localJson["cord_sha"] = row["sha"]
        if row["license"] is not None and len(row["license"])>0:
            localJson["license-simplified"] = row["license"]
        if row["abstract"] is not None and len(row["abstract"])>0:
            localJson["abstract"] = row["abstract"]
        if row["mag_id"] is not None and len(row["mag_id"])>0:    
            localJson["MAG_ID"] = row["mag_id"]
        if row["who_covidence_id"] is not None and len(row["who_covidence_id"])>0:      
            localJson["WHO_Covidence"] = row["who_covidence_id"]
        
        if 'DOI' not in localJson and row["doi"] is not none and len(row["doi"])>0:
            localJson['DOI'] = row["doi"]
        
        if 'DOI' in localJson:
            print("processing", localJson['DOI'], "as", identifier)
        else:
            print("processing", identifier)

        # add possible missing information in the metadata entry
        if row["pmcid"] is not None and len(row["pmcid"])>0 and 'pmcid' not in localJson:
            localJson['pmcid'] = row["pmcid"]
        if row["pubmed_id"] is not None and len(row["pubmed_id"])>0 and 'pmid' not in localJson:
            localJson['pmid'] = row["pubmed_id"]
        if row["arxiv_id"] is not None and len(row["arxiv_id"])>0 and 'arxiv_id' not in localJson:
            localJson['arxiv_id'] = row["arxiv_id"]

        localJson = _initProcessStateInformation(localJson)

        # update uuid lookup map
        with self.env_uuid.begin(write=True) as txn_uuid:
            txn_uuid.put(row["cord_uid"].encode(encoding='UTF-8'), identifier.encode(encoding='UTF-8'))

        self.updateIdentifierMap(localJson)
        self.processTask(localJson)

    def updateIdentifierMap(self, localJson):
        if "DOI" in localJson:
            with self.env_uuid.begin(write=True) as txn_uuid:
                txn_uuid.put(localJson['DOI'].encode(encoding='UTF-8'), localJson["id"].encode(encoding='UTF-8'))
        if "pmcid" in localJson:
            with self.env_uuid.begin(write=True) as txn_uuid:
                txn_uuid.put(localJson['pmcid'].encode(encoding='UTF-8'), localJson["id"].encode(encoding='UTF-8'))
        if "pmid" in localJson:
            with self.env_uuid.begin(write=True) as txn_uuid:
                txn_uuid.put(localJson['pmid'].encode(encoding='UTF-8'), localJson["id"].encode(encoding='UTF-8'))

    def processTask(self, localJson):
        identifier = localJson["id"]

        # call Unpaywall
        localUrl = None
        if not localJson["has_valid_oa_url"] or not localJson["has_valid_pdf"]:
            # for PMC, we can use NIH ftp server for retrieving the PDF and XML NLM file
            if "pmcid" in localJson:
                localUrl, _ = self.pmc_oa_check(pmcid=localJson["pmcid"])

            if localUrl is None:
                # for CORD-19, we test if we have an Elsevier OA publication, if yes we can check the local PDF store 
                # obtained from the Elsevier COVID-19 ftp
                if "pii" in localJson:
                    local_pii = localJson['pii']
                else:
                    local_pii = None
                if "DOI" in localJson:
                    local_doi = localJson['DOI'].lower() 
                else:
                    local_doi = None
                local_elsevier = self.elsevier_oa_check(doi=local_doi,pii=local_pii)
                if local_elsevier is not None and os.path.isfile(local_elsevier):
                    localUrl = "file://" + local_elsevier

            if localUrl is None:
                try:
                    localUrl = self.unpaywalling_doi(localJson['DOI'])
                except:
                    print("Unpaywall API call for finding Open URL not succesful")   
                    
            if localUrl is None or len(localUrl) == 0:
                if "oaLink" in localJson:
                    # we can try to use the OA link from bibilio-glutton as fallback (though not very optimistic on this!)
                    localUrl = localJson["oaLink"]
            else:
                localJson["oaLink"] = localUrl

            if "oaLink" in localJson and localJson["oaLink"] is not None and len(localJson["oaLink"])>0:
                localJson["has_valid_oa_url"] = True

        if "oaLink" in localJson:
            print(localJson["oaLink"])

        # let's try to get this damn PDF
        pdf_filename = os.path.join(self.config["data_path"], identifier+".pdf")
        if not os.path.exists(pdf_filename):
            dest_path = generateStoragePath(identifier)
            pdf_filename2 = os.path.join(self.config["data_path"], dest_path, identifier+".pdf")
            if os.path.exists(pdf_filename2):
                # copy to working area
                shutil.copyfile(pdf_filename2, pdf_filename)

        if not localJson["has_valid_pdf"]:# or not localJson["has_valid_tei"] or (self.thumbnail and not localJson["has_valid_thumbnail"]):
            if "oaLink" in localJson:
                localUrl = localJson["oaLink"]
                if localUrl is not None and len(localUrl)>0:
                    if localUrl.startswith("file://") and os.path.isfile(localUrl.replace("file://","")):
                        shutil.copyfile(localUrl.replace("file://",""), pdf_filename)
                    elif localUrl.endswith(".tar.gz"):
                        archive_file = os.path.join(self.config["data_path"], identifier+".tar.gz")
                        _download(localUrl, archive_file)
                        _manage_pmc_archives(archive_file)
                    else:
                        _download(localUrl, pdf_filename)
                    if _is_valid_file(pdf_filename, "pdf"):
                        localJson["has_valid_pdf"] = True

        # GROBIDification if PDF available and we don't limit ourself to just download
        if not localJson["has_valid_tei"] and not self.only_download:
            tei_filename = os.path.join(self.config["data_path"], identifier+".grobid.tei.xml")
            annotation_filename = None
            if self.annotation:
                annotation_filename = os.path.join(self.config["data_path"], identifier+"-ref-annotations.json")
            if localJson["has_valid_pdf"]:
                # GROBIDification with full biblio consolidation
                try:
                    self.run_grobid(pdf_filename, tei_filename, annotation_filename)
                except:
                    print("Grobid call failed")    
                if _is_valid_file(tei_filename, "xml"):
                    localJson["has_valid_tei"] = True
                if self.annotation and _is_valid_file(annotation_filename, "json"):
                    localJson["has_valid_ref_annotation"] = True

        # thumbnail if requested 
        if not localJson["has_valid_thumbnail"] and self.thumbnail:
            if localJson["has_valid_pdf"]:
                generate_thumbnail(pdf_filename)
                if _is_valid_file(pdf_filename.replace('.pdf', '-thumb-small.png'), "png"):
                    localJson["has_valid_thumbnail"] = True

        # write the consolidated metadata in the working data directory 
        with open(os.path.join(self.config["data_path"],identifier+".json"), "w") as file_out:
            jsonStr = json.dumps(localJson, sort_keys=True)
            file_out.write(jsonStr)

        # and in the entry lmdb for the final dump (avoid retrieving the article metadata over S3 if set)
        with self.env_entries.begin(write=True) as txn2:
            txn2.put(identifier.encode(encoding='UTF-8'), _serialize_pickle(localJson))  

        # finalize by moving the downloaded and generated files to storage
        self.manageFiles(localJson)


    def manageFiles(self, local_entry):
        """
        If S3 is the target storage, we upload the data for an article to the specified S3 bucket
        and keep it clean behind us in the local data path.
        Otherwise we simply move the data files under a tree structure adapted to a large number of files
        """
        local_filename_pdf = os.path.join(self.config["data_path"], local_entry['id']+".pdf")
        local_filename_nxml = os.path.join(self.config["data_path"], local_entry['id']+".nxml")
        local_filename_tei = os.path.join(self.config["data_path"], local_entry['id']+".grobid.tei.xml")
        local_filename_json = os.path.join(self.config["data_path"], local_entry['id']+".json")
        local_filename_ref = os.path.join(self.config["data_path"], local_entry['id']+"-ref-annotations.json")

        dest_path = generateStoragePath(local_entry['id'])
        thumb_file_small = local_filename_pdf.replace('.pdf', '-thumb-small.png')
        thumb_file_medium = local_filename_pdf.replace('.pdf', '-thumb-medium.png')
        thumb_file_large = local_filename_pdf.replace('.pdf', '-thumb-large.png')

        if self.s3 is not None:
            # upload to S3 
            # upload is already in parallel for individual file (with parts)
            # so we don't further upload in parallel at the level of the files
            if os.path.isfile(local_filename_pdf) and _is_valid_file(local_filename_pdf, "pdf"):
                self.s3.upload_file_to_s3(local_filename_pdf, dest_path, storage_class='ONEZONE_IA')
            if os.path.isfile(local_filename_nxml):
                self.s3.upload_file_to_s3(local_filename_nxml, dest_path, storage_class='ONEZONE_IA')
            if os.path.isfile(local_filename_tei):
                self.s3.upload_file_to_s3(local_filename_tei, dest_path, storage_class='ONEZONE_IA')
            if os.path.isfile(local_filename_json):
                self.s3.upload_file_to_s3(local_filename_json, dest_path, storage_class='ONEZONE_IA')
            if os.path.isfile(local_filename_ref):
                self.s3.upload_file_to_s3(local_filename_ref, dest_path, storage_class='ONEZONE_IA')

            if (self.thumbnail):
                if os.path.isfile(thumb_file_small):
                    self.s3.upload_file_to_s3(thumb_file_small, dest_path, storage_class='ONEZONE_IA')

                if os.path.isfile(thumb_file_medium): 
                    self.s3.upload_file_to_s3(thumb_file_medium, dest_path, storage_class='ONEZONE_IA')
                
                if os.path.isfile(thumb_file_large): 
                    self.s3.upload_file_to_s3(thumb_file_large, dest_path, storage_class='ONEZONE_IA')
        else:
            # save under local storate indicated by data_path in the config json
            try:
                local_dest_path = os.path.join(self.config["data_path"], dest_path)
                os.makedirs(os.path.dirname(local_dest_path), exist_ok=True)
                if os.path.isfile(local_filename_pdf) and _is_valid_file(local_filename_pdf, "pdf"):
                    shutil.copyfile(local_filename_pdf, os.path.join(local_dest_path, local_entry['id']+".pdf"))
                if os.path.isfile(local_filename_nxml):
                    shutil.copyfile(local_filename_nxml, os.path.join(local_dest_path, local_entry['id']+".nxml"))
                if os.path.isfile(local_filename_tei):
                    shutil.copyfile(local_filename_tei, os.path.join(local_dest_path, local_entry['id']+".grobid.tei.xml"))
                if os.path.isfile(local_filename_json):
                    shutil.copyfile(local_filename_json, os.path.join(local_dest_path, local_entry['id']+".json"))
                if os.path.isfile(local_filename_ref):
                    shutil.copyfile(local_filename_ref, os.path.join(local_dest_path, local_entry['id']+"-ref-annotations.json"))

                if (self.thumbnail):
                    if os.path.isfile(thumb_file_small):
                        shutil.copyfile(thumb_file_small, os.path.join(local_dest_path, local_entry['id']+"-thumb-small.png"))

                    if os.path.isfile(thumb_file_medium):
                        shutil.copyfile(thumb_file_medium, os.path.join(local_dest_path, local_entry['id']+"-thumb-medium.png"))

                    if os.path.isfile(thumb_file_large):
                        shutil.copyfile(thumb_file_large, os.path.join(local_dest_path, local_entry['id']+"-thumb-larger.png"))

            except IOError as e:
                print("invalid path", str(e))       

        # clean pdf and thumbnail files
        try:
            if os.path.isfile(local_filename_pdf):
                os.remove(local_filename_pdf)
            if os.path.isfile(local_filename_nxml):
                os.remove(local_filename_nxml)
            if os.path.isfile(local_filename_tei):
                os.remove(local_filename_tei)
            if os.path.isfile(local_filename_json):
                os.remove(local_filename_json)
            if os.path.isfile(local_filename_ref):
                os.remove(local_filename_ref)
            if (self.thumbnail):
                if os.path.isfile(thumb_file_small): 
                    os.remove(thumb_file_small)
                if os.path.isfile(thumb_file_medium): 
                    os.remove(thumb_file_medium)
                if os.path.isfile(thumb_file_large): 
                    os.remove(thumb_file_large)
        except IOError as e:
            print("temporary file cleaning failed:", str(e))    

    def getUUIDByStrongIdentifier(self, strong_identifier):
        """
        Strong identifiers depend on the data to be processed but typically includes DOI, sha, PMID, PMCID
        """
        txn = self.env_uuid.begin()
        return txn.get(strong_identifier.encode(encoding='UTF-8'))

    def diagnostic(self, full=False):
        """
        Print a report on failures stored during the harvesting process
        """
        nb_total = 0
        nb_invalid_oa_url = 0
        nb_invalid_pdf = 0  
        nb_invalid_tei = 0  
        nb_total_valid = 0
        with self.env_entries.begin(write=True) as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                nb_total += 1
                localJson = _deserialize_pickle(value)
                if not localJson["has_valid_oa_url"]:
                    nb_invalid_oa_url += 1
                    nb_invalid_pdf += 1
                    nb_invalid_tei += 1
                elif not localJson["has_valid_pdf"]:
                    nb_invalid_pdf += 1
                    nb_invalid_tei += 1
                elif not localJson["has_valid_tei"]:
                    nb_invalid_tei += 1
                else:
                    nb_total_valid += 1

        print("---")
        print("total entries:", nb_total)
        print("---")
        print("total valid entries:", nb_total_valid, "entries with valid OA URL and PDF and TEI XML")
        print("---")
        print("total invalid OA URL:", nb_invalid_oa_url)
        print("total entries with valid OA URL:", str(nb_total-nb_invalid_oa_url))
        print("---")
        print("total invalid PDF:", nb_invalid_pdf)
        print("total entries with successfully downloaded PDF:", str(nb_total-nb_invalid_pdf))
        print("---")
        print("total invalid TEI:", nb_invalid_tei)
        print("total entries with successfully convereted TEI XML:", str(nb_total-nb_invalid_tei))
        print("---")

        if full:
            # check if we have the identifier map entries not present in the metadata map (this would indicate
            # some sort of silent failure in the process, having no aggregated metadata saved)

            nb_missing_metadata_entry = 0
            nb_total_identifiers = 0
            identifiers = set()
            # iterate over the identifier lmdb
            with self.env_uuid.begin(write=True) as txn:
                cursor = txn.cursor()
                for key, value in cursor:
                    decoded_value = value.decode(encoding='UTF-8')
                    if decoded_value not in identifiers:
                        identifiers.add(decoded_value)
                        nb_total_identifiers += 1
                    # do we have a corresponding entry?
                    with self.env_entries.begin(write=False) as txn2:
                        metadata_object = txn2.get(value)
                        if not metadata_object:
                            nb_missing_metadata_entry += 1
            
            print("total identifiers:", nb_total_identifiers)
            print("total missing entries in metadata map:", str(nb_missing_metadata_entry))
            print("---")

            # check the presence of the TEI files, from Grobid, Pub2TEI and the entries with at least one
            # TEI XML file - walk through the data directory
            nb_tei_present = 0
            nb_grobid_tei_present = 0
            nb_pub2tei_tei_present = 0
            for root, dirs, files in os.walk(self.config["data_path"]):
                for the_file in files:
                    if the_file.endswith(".json"):
                        # we have an entry normally, check if we have a TEI file
                        grobid_tei_file = os.path.join(root,the_file.replace(".json", ".grobid.tei.xml"))
                        pub2tei_tei_file = os.path.join(root,the_file.replace(".json", ".pub2tei.tei.xml"))
                        if os.path.isfile(grobid_tei_file) or os.path.isfile(pub2tei_tei_file):
                            nb_tei_present += 1
                        if os.path.isfile(grobid_tei_file):
                            nb_grobid_tei_present += 1
                        if os.path.isfile(pub2tei_tei_file):
                            nb_pub2tei_tei_present += 1

            print("total entries with GROBID TEI file:", str(nb_grobid_tei_present))
            print("total entries with Pub2TEI TEI file:", str(nb_pub2tei_tei_present))
            print("total entries with at least one TEI file:", str(nb_tei_present))
            print("---")

    def reprocessFailed(self):
        localJsons = []
        i = 0
        # iterate over the entry lmdb
        with self.env_entries.begin(write=False) as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if i == self.config["batch_size"]:
                    with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                        executor.map(self.processTask, localJsons)
                    # reinit
                    i = 0
                    localJsons = []

                localJson = _deserialize_pickle(value)
                if not localJson["has_valid_oa_url"] or not localJson["has_valid_pdf"] or not localJson["has_valid_tei"]:
                    localJsons.append(localJson)
                    i += 1
                    print("re-processing", localJson["id"])
                elif self.thumbnail and not localJson["has_valid_thumbnail"]:
                    localJsons.append(localJson)
                    i += 1
                    print("re-processing for thumbnails", localJson["id"])
                elif self.annotation and not localJson["has_valid_ref_annotation"]:
                    localJsons.append(localJson)
                    i += 1
                    print("re-processing for PDF annotations", localJson["id"])

        # we need to process the latest incomplete batch (if not empty)
        if len(localJsons)>0:
            with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                executor.map(self.processTask, localJsons)


def _serialize_pickle(a):
    return pickle.dumps(a)

def _deserialize_pickle(serialized):
    return pickle.loads(serialized)

def _clean_doi(doi):
    if doi.startswith("https://doi.org/10."):
        doi = doi.replace("https://doi.org/", "")
    elif doi.startswith("http://dx.doi.org/10."): 
        doi = doi.replace("http://dx.doi.org/", "")
    return doi.strip().lower()

def _check_compression(file):
    '''
    check if a file is compressed, if yes decompress and replace by the decompressed version
    '''
    if os.path.isfile(file):
        if os.path.getsize(file) == 0:
            return False
        file_type = magic.from_file(file, mime=True)
        if file_type == 'application/gzip':
            success = False
            # decompressed in tmp file
            with gzip.open(file, 'rb') as f_in:
                with open(file+'.decompressed', 'wb') as f_out:
                    try:
                        shutil.copyfileobj(f_in, f_out)
                    except OSError:  
                        print ("Decompression file failed:", f_in)
                    else:
                        success = True
            # replace the file
            if success:
                try:
                    shutil.copyfile(file+'.decompressed', file)
                except OSError:  
                    print ("Replacement of decompressed file failed:", file)
                    success = False
            # delete the tmp file
            if os.path.isfile(file+'.decompressed'):
                try:
                    os.remove(file+'.decompressed')
                except OSError:  
                    print ("Deletion of temp decompressed file failed:", file+'.decompressed')    
            return success
        else:
            return True
    return False

def _is_valid_file(file, mime_type):
    target_mime = []
    if mime_type == 'xml':
        target_mime.append("application/xml")
        target_mime.append("text/xml")
    elif mime_type == 'png':
        target_mime.append("image/png")
    else:
        target_mime.append("application/"+mime_type)
    file_type = ""
    if os.path.isfile(file):
        if os.path.getsize(file) == 0:
            return False
        file_type = magic.from_file(file, mime=True)
    return file_type in target_mime

def _initProcessStateInformation(json_entry):
    # init process information
    json_entry["has_valid_pdf"] = False
    json_entry["has_valid_oa_url"] = False
    json_entry["has_valid_tei"] = False
    json_entry["has_valid_ref_annotation"] = False
    json_entry["has_valid_thumbnail"] = False
    return json_entry

def _biblio_glutton_url(biblio_glutton_base, biblio_glutton_port):
    if biblio_glutton_base.endswith("/"):
        res = biblio_glutton_base[:-1]
    else: 
        res = biblio_glutton_base
    if biblio_glutton_port is not None and len(biblio_glutton_port)>0:
        res += ":"+biblio_glutton_port
    return res+"/service/lookup?"

def _grobid_url(grobid_base, grobid_port):
    the_url = 'http://'+grobid_base
    if grobid_port is not None and len(grobid_port)>0:
        the_url += ":"+grobid_port
    the_url += "/api/"
    return the_url

def _download(url, filename):
    result = _download_wget(url, filename)
    if result != "success":
        result = _download_requests(url, filename)
    return result

def _download_wget(url, filename):
    """ 
    First try with Python requests (which handle well compression), then move to a more robust download approach
    """
    result = "fail"
    # This is the most robust and reliable way to download files I found with Python... to rely on system wget :)
    #cmd = "wget -c --quiet" + " -O " + filename + ' --connect-timeout=10 --waitretry=10 ' + \
    cmd = "wget -c --quiet" + " -O " + filename + ' --timeout=5 --waitretry=0 --tries=10 --retry-connrefused ' + \
        '--header="User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0" ' + \
        '--header="Accept: application/pdf, text/html;q=0.9,*/*;q=0.8" --header="Accept-Encoding: gzip, deflate" ' + \
        '--no-check-certificate ' + \
        '"' + url + '"'
    #print(cmd)
    try:
        result = subprocess.check_call(cmd, shell=True)
        
        # if the used version of wget does not decompress automatically, the following ensures it is done
        result_compression = _check_compression(filename)
        if not result_compression:
            # decompression failed, or file is invalid
            if os.path.isfile(filename):
                try:
                    os.remove(filename)
                except OSError:
                    print ("Deletion of invalid compressed file failed:", filename) 
                    result = "fail"
            # ensure cleaning
            if os.path.isfile(filename+'.decompressed'):
                try:
                    os.remove(filename+'.decompressed')
                except OSError:  
                    print ("Final deletion of temp decompressed file failed:", filename+'.decompressed')    
        else:
            result = "success"

    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)
        print("e.output", e.output)
        print("wget command was: "+cmd)
        #if e.output is not None and e.output.startswith('error: {'):
        if  e.output is not None:
            error = json.loads(e.output[7:]) # Skip "error: "
            print("error code:", error['code'])
            print("error message:", error['message'])
        result = "fail"

    except Exception as e:
        # a bit of bad practice
        print("Unexpected error wget process", e)
        result = "fail"

    return str(result)

def _download_requests(url, filename):
    """ 
    Download with Python requests which handle well compression, but not very robust and bad parallelization
    """
    HEADERS = {"""User-Agent""": """Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0"""}
    result = "fail" 
    try:
        file_data = requests.get(url, allow_redirects=True, headers=HEADERS)
        if file_data.status_code == 200:
            with open(filename, 'wb') as f_out:
                f_out.write(file_data.content)
            result = "success"
    except Exception:
        print("Download failed for {0} with requests".format(url))
    return result

def _manage_pmc_archives(filename):
    # check if finename exists and we have downloaded an archive rather than a PDF (case ftp PMC)
    if os.path.exists(filename) and os.path.isfile(filename) and filename.endswith(".tar.gz"):
        try:
            # for PMC we still have to extract the PDF from archive
            #print(filename, "is an archive")
            thedir = os.path.dirname(filename)
            # we need to extract the PDF, the NLM extra file, change file name and remove the tar file
            tar = tarfile.open(filename)
            pdf_found = False
            # this is a unique temporary subdirectory to extract the relevant files in the archive, unique directory is
            # introduced to avoid several files with the same name from different archives to be extracted in the 
            # same place 
            basename = os.path.basename(filename)
            tmp_subdir = basename[0:6]
            for member in tar.getmembers():
                if not pdf_found and member.isfile() and (member.name.endswith(".pdf") or member.name.endswith(".PDF")):
                    member.name = os.path.basename(member.name)
                    # create unique subdirectory
                    if not os.path.exists(os.path.join(thedir,tmp_subdir)):
                        os.mkdir(os.path.join(thedir,tmp_subdir))
                    f = tar.extract(member, path=os.path.join(thedir,tmp_subdir))
                    #print("extracted file:", member.name)
                    # be sure that the file exists (corrupted archives are not a legend)
                    if os.path.isfile(os.path.join(thedir,tmp_subdir,member.name)):
                        os.rename(os.path.join(thedir,tmp_subdir,member.name), filename.replace(".tar.gz", ".pdf"))                        
                        pdf_found = True
                    # delete temporary unique subdirectory
                    try:
                        shutil.rmtree(os.path.join(thedir,tmp_subdir))
                    except OSError:  
                        print ("Deletion of tmp dir failed:", os.path.join(thedir,tmp_subdir))     
                    #break
                if member.isfile() and member.name.endswith(".nxml"):
                    member.name = os.path.basename(member.name)
                    # create unique subdirectory
                    if not os.path.exists(os.path.join(thedir,tmp_subdir)):
                        os.mkdir(os.path.join(thedir,tmp_subdir))
                    f = tar.extract(member, path=os.path.join(thedir,tmp_subdir))
                    #print("extracted file:", member.name)
                    # be sure that the file exists (corrupted archives are not a legend)
                    if os.path.isfile(os.path.join(thedir,tmp_subdir,member.name)):
                        os.rename(os.path.join(thedir,tmp_subdir,member.name), filename.replace(".tar.gz", ".nxml"))
                    # delete temporary unique subdirectory
                    try:
                        shutil.rmtree(os.path.join(thedir,tmp_subdir))
                    except OSError:  
                        print ("Deletion of tmp dir failed:", os.path.join(thedir,tmp_subdir))      
            tar.close()
            if not pdf_found:
                print("warning: no pdf found in archive:", filename)
            if os.path.isfile(filename):
                try:
                    os.remove(filename)
                except OSError:  
                    print ("Deletion of PMC archive file failed:", filename) 
        except Exception as e:
            # a bit of bad practice
            print("Unexpected error", e)
            pass

def generate_thumbnail(pdfFile):
    """
    Generate a PNG thumbnails (3 different sizes) for the front page of a PDF. 
    Use ImageMagick for this.
    """
    thumb_file = pdfFile.replace('.pdf', '-thumb-small.png')
    cmd = 'convert -quiet -density 200 -thumbnail x150 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)

    thumb_file = pdfFile.replace('.pdf', '-thumb-medium.png')
    cmd = 'convert -quiet -density 200 -thumbnail x300 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)

    thumb_file = pdfFile.replace('.pdf', '-thumb-large.png')
    cmd = 'convert -quiet -density 200 -thumbnail x500 -flatten ' + pdfFile+'[0] ' + thumb_file
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)

def generateStoragePath(identifier):
    '''
    Convert an identifier name into a path with file prefix as directory paths:
    123456789 -> 12/34/56/123456789
    '''
    return os.path.join(identifier[:2], identifier[2:4], identifier[4:6], identifier[6:8], identifier, "")

def test():
    harvester = Harverster()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "COVIDataset harvester")
    parser.add_argument("--dois", default=None, help="path to a file describing a dataset articles as a simple list of DOI (one per line)") 
    parser.add_argument("--cord19", default=None, help="path to the csv file describing the CORD-19 dataset articles") 
    parser.add_argument("--pmids", default=None, help="path to a file describing a dataset articles as a simple list of PMID (one per line)") 
    parser.add_argument("--pmcids", default=None, help="path to a file describing a dataset articles as a simple list of PMC ID (one per line)") 
    parser.add_argument("--config", default="./config.json", help="path to the config file, default is ./config.json") 
    parser.add_argument("--reset", action="store_true", help="ignore previous processing states, and re-init the harvesting process from the beginning") 
    parser.add_argument("--reprocess", action="store_true", help="reprocessed existing failed entries") 
    parser.add_argument("--thumbnail", action="store_true", help="generate thumbnail files for the front page of the harvested PDF") 
    parser.add_argument("--annotation", action="store_true", help="generate bibliographical annotations with coordinates for the harvested PDF") 
    parser.add_argument("--diagnostic", action="store_true", help="perform a full consistency diagnostic on the harvesting and transformation process") 
    #parser.add_argument("--sample", type=int, default=None, help="harvest only a random sample of indicated size")
    parser.add_argument("--dump", action="store_true", help="write all the consolidated metadata in json in the file consolidated_metadata.json") 
    parser.add_argument("--download", action="store_true", help="only download the raw files (PDF, NLM/JATS) without processing them") 

    args = parser.parse_args()

    dois_path = args.dois
    pmids_path = args.pmids
    pmcids_path = args.pmcids
    csv_cord19 = args.cord19
    config_path = args.config
    reset = args.reset
    dump = args.dump
    thumbnail = args.thumbnail
    annotation = args.annotation
    reprocess = args.reprocess
    full_diagnostic = args.diagnostic
    only_download = args.download
    #sample = args.sample

    harvester = Harverster(config_path=config_path, 
        thumbnail=thumbnail, 
        sample=None, 
        dump_metadata=dump, 
        annotation=annotation, 
        only_download=only_download)

    if reset:
        if input("You asked to reset the existing harvesting, this will removed all the already downloaded data files... are you sure? (y/n) ") == "y":
            harvester.reset(True)
        else:
            print("skipping reset...")

    start_time = time.time()

    if reprocess:
        harvester.reprocessFailed()        
    elif csv_cord19:
        if not os.path.isfile(csv_cord19):
            print("error: the indicated cvs file path is not valid:", csv_cord19)
            sys.exit(0)    
        harvester.harvest_cord19(csv_cord19)
    elif dois_path:    
        if not os.path.isfile(dois_path):
            print("error: the indicated DOI file path is not valid:", dois_path)
            sys.exit(0)    
        harvester.harvest_dois(dois_path)
    elif pmids_path:    
        if not os.path.isfile(pmids_path):
            print("error: the indicated PMID file path is not valid:", pmids_path)
            sys.exit(0)    
        harvester.harvest_pmids(pmids_path)
    elif pmcids_path:    
        if not os.path.isfile(pmcids_path):
            print("error: the indicated PMC ID file path is not valid:", pmcids_path)
            sys.exit(0)    
        harvester.harvest_pmcids(pmcids_path)

    harvester.diagnostic(full=full_diagnostic)

    if dump :
        harvester.dump_metadata()

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))
