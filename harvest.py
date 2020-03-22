"""
What:
- Harvester for article set (basic metadata provided in a csv file, e.g. CORD-19 csv metadata file)
- Perform some metadata enrichment/agregation via biblio-glutton and output consolidated metadata in a json file 
- Perform Grobid full processing of PDF (including bibliographical reference consolidation and OA access resolution)

Optionally: 
- generate thumbnails for article (first page) 
- load stuff on S3 instead of local file
- generate json PDF annotation (with coordinates) for inline reference markers and bibliographical references 

Usage:

Fill the file config.json with relevant service and parameter url, then install the python mess:

> pip3 install -f requirements

and for harvesting the CORD-19 datatset (based on the provided source_metadata.csv) run: 

> python3 harvest --cord19 source_metadata.csv --out consolidated_metadata_out.json

This will generate a consolidated metadata file (--out,  or consolidated_metadata.json by default), upload full text files, 
converted tei.xml files and other optional files either in the local file system (under data_path indicated in the config.json 
file) or on a S3 bucket if the fields are filled in config.json. 

for example:

> python3 harvest --cord19 all_sources_metadata_2020-03-13.csv     

you can set a specific config file name like this:

> python3 harvest --cord19 all_sources_metadata_2020-03-13.csv --config my_config_file.json    

Structure of the generated files:

article_uuid/article_uuid.pdf
article_uuid/article_uuid.tei.xml

Optional additional files:

xx/xx/xx/xx/article_uuid/article_uuid-ref-annotations.json
xx/xx/xx/xx/article_uuid/article_uuid-thumb-small.png
xx/xx/xx/xx/article_uuid/article_uuid-thumb-medium.png
xx/xx/xx/xx/article_uuid/article_uuid-thumb-large.png

article_uuid for a particular article is given in the generated consolidated_metadata.csv file
"""

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

map_size = 100 * 1024 * 1024 * 1024 

class Harverster(object):

    def __init__(self, config_path='./config.json', thumbnail=False, sample=None, dump_metadata=None, annotation=False):
        self.config = None   
        self._load_config(config_path)

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

        # standard lmdb environment for storing biblio entries by uuid
        self.env_entries = None

        # lmdb environment for storing mapping between sha/doi/pmcid and uuid
        self.env_uuid = None

        # lmdb environment for keeping track of failures
        #self.env_fail = None

        self._init_lmdb()

    def _load_config(self, path='./config.json'):
        """
        Load the json configuration 
        """
        config_json = open(path).read()
        self.config = json.loads(config_json)

        # test if GROBID is up and running...
        the_url = _grobid_url(self.config['grobid_base'], self.config['grobid_port'])
        the_url += "isalive"
        r = requests.get(the_url)
        if r.status_code != 200:
            print('GROBID server does not appear up and running ' + str(r.status_code))
        else:
            print("GROBID server is up and running")

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

        #envFilePath = os.path.join(self.config["data_path"], 'fail')
        #self.env_fail = lmdb.open(envFilePath, map_size=map_size)

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

    def biblio_glutton_lookup(self, doi=None, pmcid=None, pmid=None, istex_id=None):
        """
        Lookup on biblio_glutton with the provided strong identifiers, return the full agregated biblio_glutton record
        """
        biblio_glutton_url = _biblio_glutton_url(self.config["biblio_glutton_base"], self.config["biblio_glutton_port"])

        if doi is not None and len(doi)>0:
            print(biblio_glutton_url + "doi=" + doi)
            response = requests.get(biblio_glutton_url, params={'doi': doi})

        if response.status_code != 200 and pmid is not None and len(pmid)>0:
            response = requests.get(biblio_glutton_url + "pmid=" + pmid)

        if response.status_code != 200 and pmcid is not None and len(pmcid)>0:
            response = requests.get(biblio_glutton_url + "pmc=" + pmcid)  

        if response.status_code != 200 and istex_id is not None and len(istex_id)>0:
            response = requests.get(biblio_glutton_url + "istexid=" + istex_id)

        jsonResult = None
        if response.status_code == 200:
            jsonResult = response.json()
        else:
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
        
        return jsonResult

    def reset(self, dump_file=None):
        """
        Remove the local files and lmdb keeping track of the state of advancement of the harvesting and
        of the failed entries
        """
        # close environments
        self.env_entries.close()
        self.env_uuid.close()
        #self.env_fail.close()

        envFilePath = os.path.join(self.config["data_path"], 'entries')
        shutil.rmtree(envFilePath)

        envFilePath = os.path.join(self.config["data_path"], 'uuid')
        shutil.rmtree(envFilePath)

        #envFilePath = os.path.join(self.config["data_path"], 'fail')
        #shutil.rmtree(envFilePath)

        # clean any possibly remaining tmp files
        for f in os.listdir(self.config["data_path"]):
            if f.endswith(".pdf") or f.endswith(".png") or f.endswith(".nxml") or f.endswith(".xml") or f.endswith(".tar.gz") or f.endswith(".json"):
                os.remove(os.path.join(self.config["data_path"], f))

            # clean any existing data files
            path = os.path.join(self.config["data_path"], f)
            if os.path.isdir(path):
                try:
                    shutil.rmtree(path)
                except OSError as e:
                    print("Error: %s - %s." % (e.filename, e.strerror))

        # clean the metadata file if present
        if self.dump_file is None:
            self.dump_file = "consolidated_metadata.json" 
        if os.path.isfile(self.dump_file):
            os.remove(self.dump_file)

        # re-init the environments
        self._init_lmdb()

    def dump_metadata(self):
        if self.dump_file is None:
            self.dump_file = "consolidated_metadata.json"

        # init lmdb transactions
        txn = self.env_entries.begin(write=True)
        
        nb_total = txn.stat()['entries']
        print("number of harvested entries:", nb_total)

        with open(self.dump_file,'w') as file_out:
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
            if os.path.isfile(self.dump_file):
                self.s3.upload_file_to_s3(self.dump_file, ".", storage_class='ONEZONE_IA')


    def run_grobid(self, pdf_file, output=None, annotation_output=None):
        # normal fulltext TEI file
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
                   pass

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
                   pass

    def harvest_dois(self, dois_file):
        with open(filepath, 'rt') as fp:
            line_count = 0 # total count of articles
            i = 0 # counter for article per batch
            identifiers = []
            dois = []  
            for count, line in enumerate(fp):

                if i == self.config["batch_size"]:
                    with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                        # branch to the right entry processor, depending on the input csv 
                        executor.map(self.processEntryDOI, identifiers, dois)
                    # reinit
                    i = 0
                    identifiers = []
                    dois = []

                # check if the entry has already been processed
                if self.getUUIDByStrongIdentifier(row["sha"]) is not None:
                    continue

                # we need a new identifier
                identifier = str(uuid.uuid4())
                identifiers.append(identifier)
                dois.append(line.trim())
    
                line_count += 1
                i += 1
            
            # we need to process the last incomplete batch, if not empty
            if len(identifiers) >0:
                with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                    # branch to the right entry processor, depending on the input csv 
                    executor.map(self.processEntryDOI, identifiers, dois)

            print("processed", str(line_count), "articles")

    def harvest_csv(self, metadata_csv_file):
        with open(metadata_csv_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0 # total count of articles
            i = 0 # counter for article per batch
            identifiers = []
            rows = []
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                    continue

                if i == self.config["batch_size"]:
                    with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                        # branch to the right entry processor, depending on the input csv 
                        executor.map(self.processEntryCord19, identifiers, rows)
                    # reinit
                    i = 0
                    identifiers = []
                    rows = []

                # check if the entry has already been processed
                if self.getUUIDByStrongIdentifier(row["sha"]) is not None:
                    continue

                # we need a new identifier
                identifier = str(uuid.uuid4())
                identifiers.append(identifier)
                rows.append(row)
    
                line_count += 1
                i += 1
            
            # we need to process the last incomplete batch, if not empty
            if len(identifiers) >0:
                with ThreadPoolExecutor(max_workers=self.config["batch_size"]) as executor:
                    # branch to the right entry processor, depending on the input csv 
                    executor.map(self.processEntryCord19, identifiers, rows)

            print("processed", str(line_count), "articles")

    def processEntryDOI(self, identifier, doi):
        localJson = self.biblio_glutton_lookup(doi=_clean_doi(doi), pmcid=None, pmid=None, istex_id=None)
        if localJson is None:
            localJson = {}
            localJson['DOI'] = doi

        print("processing", localJson['DOI'], "as", identifier)

        # init process information
        localJson["has_valid_pdf"] = False
        localJson["has_valid_oa_url"] = False
        localJson["has_valid_tei"] = False
        localJson["has_valid_ref_annotation"] = False
        localJson["has_valid_thumbnail"] = False

        with self.env_uuid.begin(write=True) as txn_uuid:
            txn_uuid.put(localJson['DOI'].encode(encoding='UTF-8'), identifier.encode(encoding='UTF-8'))

        self.processTask(localJson)
            

    def processEntryCord19(self, identifier, row):
        # sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,Microsoft Academic Paper ID,WHO #Covidence,has_full_text
        localJson = self.biblio_glutton_lookup(doi=_clean_doi(row["doi"]), pmcid=row["pmcid"], pmid=row["pubmed_id"], istex_id=None)
        if localJson is None:
            localJson = {}
            localJson['title'] = row["title"]
            localJson['year']= row["publish_time"]
        
        localJson["id"] = identifier
        localJson["cord_identifier"] = row["sha"]
        if row["license"] is not None and len(row["license"])>0:
            localJson["license-simplified"] = row["license"]
        if row["abstract"] is not None and len(row["abstract"])>0:
            localJson["abstract"] = row["abstract"]
        if row["Microsoft Academic Paper ID"] is not None and len(row["Microsoft Academic Paper ID"])>0:    
            localJson["MAG_ID"] = row["Microsoft Academic Paper ID"]
        if row["WHO #Covidence"] is not None and len(row["WHO #Covidence"])>0:      
            localJson["WHO_Covidence"] = row["WHO #Covidence"]
        
        if 'DOI' not in localJson and row["doi"] is not none and len(row["doi"])>0:
            localJson['DOI'] = row["doi"]

        print("processing", localJson['DOI'], "as", identifier)

        # add possible missing information in the metadata entry
        if row["pmcid"] is not None and len(row["pmcid"])>0 and 'pmcid' not in localJson:
            localJson['pmcid'] = row["pmcid"]
        if row["pubmed_id"] is not None and len(row["pubmed_id"])>0 and 'pmid' not in localJson:
            localJson['pmid'] = row["pubmed_id"]

        # init process information
        localJson["has_valid_pdf"] = False
        localJson["has_valid_oa_url"] = False
        localJson["has_valid_tei"] = False
        localJson["has_valid_ref_annotation"] = False
        localJson["has_valid_thumbnail"] = False

        # update uuid lookup map
        with self.env_uuid.begin(write=True) as txn_uuid:
            txn_uuid.put(row["sha"].encode(encoding='UTF-8'), identifier.encode(encoding='UTF-8'))
        if "DOI" in localJson:
            with self.env_uuid.begin(write=True) as txn_uuid:
                txn_uuid.put(localJson['DOI'].encode(encoding='UTF-8'), identifier.encode(encoding='UTF-8'))
        '''
        if "pmcid" in localJson:
            with self.env_uuid.begin(write=True) as txn_uuid:
                txn_uuid.put(localJson['pmcid'].encode(encoding='UTF-8'), identifier.encode(encoding='UTF-8'))
        if "pmid" in localJson:
            with self.env_uuid.begin(write=True) as txn_uuid:
                txn_uuid.put(localJson['pmid'].encode(encoding='UTF-8'), identifier.encode(encoding='UTF-8'))
        '''
        self.processTask(localJson)


    def processTask(self, localJson):
        identifier = localJson["id"]
        print("processing", identifier)

        # call Unpaywall
        if not localJson["has_valid_oa_url"] or not localJson["has_valid_pdf"]:
            try:
                localUrl = self.unpaywalling_doi(localJson['DOI'])
            except:
                print("Failure to call Unpaywall API")   
            if localUrl is None or len(localUrl) == 0:
                if "oaLink" in localJson:
                    # we can try to use the OA link from bibilio-glutton as fallback (though not very optimistic on this!)
                    localUrl = localJson["oaLink"]
            else:
                localJson["oaLink"] = localUrl

            if localUrl is not None and len(localUrl)>0:
                localJson["has_valid_oa_url"] = True

        # let's try to get this damn PDF
        pdf_filename = os.path.join(self.config["data_path"], identifier+".pdf")
        localUrl = localJson["oaLink"]
        if not localJson["has_valid_pdf"] or not localJson["has_valid_tei"] or (self.thumbnail and not localJson["has_valid_thumbnail"]):
            if localUrl is not None and len(localUrl)>0:
                print(localUrl)
                _download(localUrl, pdf_filename)
                if _is_valid_file(pdf_filename, "pdf"):
                    localJson["has_valid_pdf"] = True
            '''
            else:
                with self.env_fail.begin(write=True) as txn_fail:
                    txn_fail.put(identifier.encode(encoding='UTF-8'), "Invalid URL".encode(encoding='UTF-8'))
            '''

        # GROBIDification if PDF available 
        if not localJson["has_valid_tei"]:
            tei_filename = os.path.join(self.config["data_path"], identifier+".tei.xml")
            annotation_filename = None
            if self.annotation:
                annotation_filename = os.path.join(self.config["data_path"], identifier+"-ref-annotations.json")
            if localJson["has_valid_pdf"]:
                # GROBIDification with full biblio consolidation
                self.run_grobid(pdf_filename, tei_filename, annotation_filename)
                if _is_valid_file(tei_filename, "xml"):
                    localJson["has_valid_tei"] = True
                '''
                else:
                    with self.env_fail.begin(write=True) as txn_fail:
                        txn_fail.put(identifier.encode(encoding='UTF-8'), "Invalid TEI".encode(encoding='UTF-8'))
                '''
                if self.annotation and _is_valid_file(annotation_filename, "json"):
                    localJson["has_valid_ref_annotation"] = True
                    # remove the entry in fail, as it is now sucessful
                    '''
                    with self.env_fail.begin(write=True) as txn_fail2:
                        txn_fail2.delete(identifier.encode(encoding='UTF-8'))
                    '''

        # thumbnail if requested 
        if not localJson["has_valid_thumbnail"] and self.thumbnail:
            if localJson["has_valid_pdf"]:
                generate_thumbnail(pdf_filename)
                if _is_valid_file(tei_filename, "png"):
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
        local_filename_tei = os.path.join(self.config["data_path"], local_entry['id']+".tei.xml")
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
            if os.path.isfile(local_filename_pdf):
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
                if os.path.isfile(local_filename_pdf):
                    shutil.copyfile(local_filename_pdf, os.path.join(local_dest_path, local_entry['id']+".pdf"))
                if os.path.isfile(local_filename_nxml):
                    shutil.copyfile(local_filename_nxml, os.path.join(local_dest_path, local_entry['id']+".nxml"))
                if os.path.isfile(local_filename_tei):
                    shutil.copyfile(local_filename_tei, os.path.join(local_dest_path, local_entry['id']+".tei.xml"))
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

    def diagnostic(self):
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
                    localJsons.append(localJson)
                    nb_invalid_tei += 1
                else:
                    nb_total_valid += 1

        print("total entries:", nb_total)
        print("total valid entries:", nb_total_valid)
        print("total invalid OA URL:", nb_invalid_oa_url)
        print("total invalid PDF:", nb_invalid_pdf)
        print("total invalid TEI:", nb_invalid_tei)

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

                '''
                failed_uuid = key.decode(encoding='UTF-8')
                reason = value.decode(encoding='UTF-8')
                print("retry failed entry:", failed_uuid, reason)
                txn_entries = self.env_entries.begin()
                metadata_value = txn_entries.get(failed_uuid.encode(encoding='UTF-8'))
                localJsons.append(_deserialize_pickle(metadata_value))
                '''
                localJson = _deserialize_pickle(value)
                if not localJson["has_valid_oa_url"] or not localJson["has_valid_pdf"] or not localJson["has_valid_tei"]:
                    localJsons.append(localJson)
                    i += 1
                elif self.thumbnail and not localJson["has_valid_thumbnail"]:
                    localJsons.append(localJson)
                    i += 1
                elif self.annotation and not localJson["has_valid_ref_annotation"]:
                    localJsons.append(localJson)
                    i += 1

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

def _is_valid_pdf(file):
    file_type = ""
    if os.path.isfile(file):
        if os.path.getsize(file) == 0:
            return False
        file_type = magic.from_file(file, mime=True)
    return "application/pdf" in file_type

def _is_valid_xml(file):
    file_type = ""
    if os.path.isfile(file):
        if os.path.getsize(file) == 0:
            return False
        file_type = magic.from_file(file, mime=True)
    return "application/xml" in file_type or "text/xml" in file_type

def _is_valid_json(file):
    file_type = ""
    if os.path.isfile(file):
        if os.path.getsize(file) == 0:
            return False
        file_type = magic.from_file(file, mime=True)
    return "application/json" in file_type

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
    """
    This is simply the most robust and reliable way to download files I found with Python... to rely on system wget :)
    """
    #cmd = "wget -c --quiet" + " -O " + filename + ' --connect-timeout=10 --waitretry=10 ' + \
    cmd = "wget -c --quiet" + " -O " + filename + '  --timeout=2 --waitretry=0 --tries=5 --retry-connrefused ' + \
        '--header="User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0" ' + \
        '--header="Accept: application/pdf, text/html;q=0.9,*/*;q=0.8" --header="Accept-Encoding: gzip, deflate" ' + \
        '"' + url + '"'
        #' --random-wait' +
    #print(cmd)
    try:
        result = subprocess.check_call(cmd, shell=True)
        #print("result:", result)
        # check if finename exists and we have downloaded an archive rather than a PDF (case ftp PMC)
        if os.path.isfile(filename) and filename.endswith(".tar.gz"):
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
                    shutil.rmtree(os.path.join(thedir,tmp_subdir))
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
                    shutil.rmtree(os.path.join(thedir,tmp_subdir))
            tar.close()
            if not pdf_found:
                print("warning: no pdf found in archive:", filename)
            if os.path.isfile(filename):
                os.remove(filename)

    except subprocess.CalledProcessError as e:   
        print("e.returncode", e.returncode)
        print("e.output", e.output)
        #if e.output is not None and e.output.startswith('error: {'):
        if  e.output is not None:
            error = json.loads(e.output[7:]) # Skip "error: "
            print("error code:", error['code'])
            print("error message:", error['message'])
            result = error['message']
        else:
            result = e.returncode

    return str(result)

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
    parser.add_argument("--config", default="./config.json", help="path to the config file, default is ./config.json") 
    parser.add_argument("--reset", action="store_true", help="ignore previous processing states, and re-init the harvesting process from the beginning") 
    parser.add_argument("--reprocess", action="store_true", help="reprocessed existing failed entries") 
    #parser.add_argument("--increment", action="store_true", help="augment an existing harvesting with an additional csv file") 
    parser.add_argument("--thumbnail", action="store_true", help="generate thumbnail files for the front page of the harvested PDF") 
    parser.add_argument("--annotation", action="store_true", help="generate bibliographical annotations with coordinates for the harvested PDF") 
    #parser.add_argument("--sample", type=int, default=None, help="harvest only a random sample of indicated size")
    parser.add_argument("--dump", default="./consolidated_metadata.json", help="write all the consolidated metadata in json") 

    args = parser.parse_args()

    dois_path = args.dois
    csv_path = args.cord19
    config_path = args.config
    reset = args.reset
    dump_out = args.dump
    thumbnail = args.thumbnail
    annotation = args.annotation
    reprocess = args.reprocess
    #sample = args.sample

    harvester = Harverster(config_path=config_path, thumbnail=thumbnail, sample=None, dump_metadata=dump_out, annotation=annotation)

    if reset:
        if input("You asked to reset the existing harvesting, this will removed all the already downloaded data files... are you sure? (y/n) ") == "y":
            harvester.reset()
        else:
            print("skipping reset...")

    start_time = time.time()

    if reprocess:
        harvester.reprocessFailed()
    elif csv_path:
        if not os.path.isfile(csv_path):
            print("error: the indicated cvs file path is not valid:", csv_path)
            sys.exit(0)
        else if csv_path:
            harvester.harvest_csv(csv_path)
        else if dois_path:    
            harvester.harvest_dois(dois_path)

    harvester.diagnostic()

    if dump_out is not None:
        harvester.dump_metadata()

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))
