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

and run: 

> python3 harvest --csv source_metadata.csv --out consolidated_metadata_out.json

This will generate a consolidated metadata file (--out,  or consolidated_metadata.json by default), upload full text files, 
converted tei.xml files and other optional files either in the local file system (under data_path indicated in the config.json 
file) or on a S3 bucket if the fields are filled in config.json. 

for example:

> python3 harvest --csv all_sources_metadata_2020-03-13.csv     

you can set a specific config file name like this:

> python3 harvest --csv all_sources_metadata_2020-03-13.csv --config my_config_file.json    

Structure of the generated files:

article_uuid/article_uuid.pdf
article_uuid/article_uuid.tei.xml

Optional additional files:

article_uuid/article_uuid_ref_annotations.json
article_uuid/article_uuid-thumb-small.png
article_uuid/article_uuid-thumb-medium.png
article_uuid/article_uuid-thumb-large.png

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
import subprocess
import S3
import csv
import time
import uuid

class Harverster(object):

    def __init__(self, config_path='./config.json', thumbnail=False, sample=None, dump_metadata=None):
        self.config = None   
        self._load_config(config_path)

        # the file where all the metadata are stored
        self.dump_file = dump_metadata

        # boolean indicating if we want to generate thumbnails of front page of PDF 
        self.thumbnail = thumbnail

        # if a sample value is provided, indicate that we only harvest the indicated number of PDF
        self.sample = sample

        self.s3 = None
        if self.config["bucket_name"] is not None and len(self.config["bucket_name"]) is not 0:
            self.s3 = S3.S3(self.config)

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
        Remove the existing local files
        """
        # clean any possibly remaining tmp files
        for f in os.listdir(self.config["data_path"]):
            if f.endswith(".pdf") or f.endswith(".png") or f.endswith(".nxml") or f.endswith(".xml") or f.endswith(".tar.gz") or f.endswith("json"):
                os.remove(os.path.join(self.config["data_path"], f))

            # clean any existing data files
            path = os.path.join(self.config["data_path"], f)
            if os.path.isdir(path):
                try:
                    shutil.rmtree(path)
                except OSError as e:
                    print("Error: %s - %s." % (e.filename, e.strerror))

        # clean the metadata file
        if self.dump_file is None:
            self.dump_file = "consolidated_metadata.json" 
        if os.path.isfile(self.dump_file):
            os.remove(self.dump_file)


    def dump_metadata(self, jsonEntry):
        if self.dump_file is None:
            self.dump_file = "consolidated_metadata.json"

        jsonStr = json.dumps(jsonEntry, sort_keys=True)
        # write the file with all the consolidated metadata
        with open(self.dump_file, "a+") as file_out:
            file_out.write(jsonStr + "\n")

        
    def run_grobid(self, pdf_file, output):
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
            return self.process_pdf(pdf_file, output)
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

    def harvest(self, metadata_csv_file):
        # read the existing metadata file for the status of entries already processed, we map the dataset identifier with cord_sha 
        digest = {}
        if os.path.isfile(self.dump_file):
            with open(self.dump_file, mode='r') as metadata_file:
                for line in metadata_file:
                    metadataJson = json.loads(line)
                    uuid_identifier = metadataJson['id']
                    if "cord_identifier" in metadataJson:
                        cord_identifier = metadataJson["cord_identifier"]
                        digest[cord_identifier] = uuid_identifier
                    # note: for adding other sources, which will have no CORD sha identifier, we will need to consider other source 
                    # identifiers here (like istex_id or hal_id)

        with open(metadata_csv_file, mode='r') as csv_file:
            # sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,Microsoft Academic Paper ID,WHO #Covidence,has_full_text
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                    continue

                if row["sha"] in digest:
                    # we have already processed this entry
                    identifier = digest[row["sha"]]
                    dest_path = generateStoragePath(identifier)
                    with open(os.path.join(self.config["data_path"],dest_path,identifier+".json")) as json_file:
                        localJson = json.load(json_file)
                else:
                    # we need a new identifier
                    identifier = str(uuid.uuid4())
                    localJson = self.biblio_glutton_lookup(doi=_clean_doi(row["doi"]), pmcid=row["pmcid"], pmid=row["pubmed_id"], istex_id=None)
                    if localJson is None:
                        localJson = {}
                        localJson['title'] = row["title"]
                        localJson['year']= row["publish_time"]
                    localJson['id'] = identifier
                    localJson["cord_identifier"] = row["sha"]
                    localJson["license-simplified"] = row["license"]
                    localJson["abstract"] = row["abstract"]
                    localJson["MAG_ID"] = row["Microsoft Academic Paper ID"]
                    localJson["WHO_Covidence"] = row["WHO #Covidence"]
                    
                    if 'DOI' not in localJson and row["doi"] is not none and len(row["doi"])>0:
                        localJson['DOI'] = row["doi"]

                    print(identifier, localJson['DOI'])
                    # add possible missing information in the metadata entry
                    if row["pmcid"] is not None and len(row["pmcid"])>0 and 'pmcid' not in localJson:
                        localJson['pmcid'] = row["pmcid"]
                    if row["pubmed_id"] is not None and len(row["pubmed_id"])>0 and 'pmid' not in localJson:
                        localJson['pmid'] = row["pubmed_id"]

                # Download PDF is not already there
                pdf_filename = os.path.join(self.config["data_path"], identifier+".pdf")
                if "has_valid_pdf" not in localJson or not localJson["has_valid_pdf"]:
                    # let's try to get this damn PDF
                    localUrl = self.unpaywalling_doi(localJson['DOI'])
                    print(localUrl)
                    if localUrl is None or len(localUrl) == 0:
                        if "oaLink" in localJson:
                            # we can try to use the OA link from bibilio-glutton as fallback (though not very optimistic on this!)
                            localUrl = localJson["oaLink"]
                    else:
                        localJson["oaLink"] = localUrl

                    if localUrl is not None and len(localUrl)>0:
                        _download(localUrl, pdf_filename)
                        if _is_valid_pdf(pdf_filename):
                            localJson["has_valid_pdf"] = True

                # GROBIDification if PDF available, and not already produced
                tei_filename = os.path.join(self.config["data_path"], identifier+".tei.xml")
                dest_path = generateStoragePath(identifier)
                stored_tei_filename = os.path.join(self.config["data_path"], dest_path, identifier+".tei.xml")
                if "has_valid_tei" not in localJson or not localJson["has_valid_tei"] or not os.path.isfile(stored_tei_filename):
                    if "has_valid_pdf" in localJson and localJson["has_valid_pdf"]:
                        # GROBIDification with full biblio consolidation
                        self.run_grobid(pdf_filename, tei_filename)
                        if _is_valid_xml(tei_filename):
                            localJson["has_valid_tei"] = True

                # thumbnail if requested and not present
                if "has_valid_pdf" in localJson and localJson["has_valid_pdf"] and self.thumbnail:
                    generate_thumbnail(pdf_filename)

                # write the consolidated metadata in the rking data directory 
                with open(os.path.join(self.config["data_path"],identifier+".json"), "w") as file_out:
                    jsonStr = json.dumps(localJson, sort_keys=True)
                    file_out.write(jsonStr)

                if row["sha"] not in digest:
                    self.dump_metadata(localJson)

                self.manageFiles(localJson)

                line_count += 1
            # optionally we need to upload to S3 the consolidated metadata file
            if self.s3 is not None:
                if os.path.isfile(self.dump_file):
                    self.s3.upload_file_to_s3(self.dump_file, ".", storage_class='ONEZONE_IA')
            print("processed", str(line_count), "articles")

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
                print(local_dest_path)
                os.makedirs(os.path.dirname(local_dest_path), exist_ok=True)
                if os.path.isfile(local_filename_pdf):
                    shutil.copyfile(local_filename_pdf, os.path.join(local_dest_path, local_entry['id']+".pdf"))
                if os.path.isfile(local_filename_nxml):
                    shutil.copyfile(local_filename_nxml, os.path.join(local_dest_path, local_entry['id']+".nxml"))
                if os.path.isfile(local_filename_tei):
                    shutil.copyfile(local_filename_tei, os.path.join(local_dest_path, local_entry['id']+".tei.xml"))
                if os.path.isfile(local_filename_json):
                    shutil.copyfile(local_filename_json, os.path.join(local_dest_path, local_entry['id']+".json"))

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
            if (self.thumbnail):
                if os.path.isfile(thumb_file_small): 
                    os.remove(thumb_file_small)
                if os.path.isfile(thumb_file_medium): 
                    os.remove(thumb_file_medium)
                if os.path.isfile(thumb_file_large): 
                    os.remove(thumb_file_large)
        except IOError as e:
            print("temporary file cleaning failed:", str(e))    

    def diagnostic(self):
        """
        Print a report on failures stored during the harvesting process
        """

def _clean_doi(doi):
    if doi.startswith("https://doi.org/10."):
        doi = doi.replace("https://doi.org/", "")
    elif doi.startswith("http://dx.doi.org/10."): 
        doi = doi.replace("http://dx.doi.org/", "")
    return doi.strip().lower()

def _is_valid_pdf(file):
    file_type = ""
    if os.path.isfile(file):
        file_type = magic.from_file(file, mime=True)
    return "application/pdf" in file_type

def _is_valid_xml(file):
    file_type = ""
    if os.path.isfile(file):
        file_type = magic.from_file(file, mime=True)
    return "application/xml" in file_type or "text/xml" in file_type

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
    parser.add_argument("--csv", default=None, help="path to the csv file describing the dataset articles") 
    #parser.add_argument("--pmc", default=None, help="path to the pmc file list, as available on NIH's site") 
    parser.add_argument("--config", default="./config.json", help="path to the config file, default is ./config.json") 
    parser.add_argument("--reset", action="store_true", help="ignore previous processing states, and re-init the harvesting process from the beginning") 
    parser.add_argument("--increment", action="store_true", help="augment an existing harvesting with an additional csv file") 
    parser.add_argument("--thumbnail", action="store_true", help="generate thumbnail files for the front page of the harvested PDF") 
    parser.add_argument("--sample", type=int, default=None, help="harvest only a random sample of indicated size")
    parser.add_argument("--out", default="./consolidated_metadata.json", help="json file name where to output all the consolidated metadata")

    args = parser.parse_args()

    #pmc = args.pmc
    csv_path = args.csv
    config_path = args.config
    reset = args.reset
    dump_out = args.out
    thumbnail = args.thumbnail
    sample = args.sample

    if not os.path.isfile(csv_path):
        print("error: the indicated cvs file path is not valid:", csv_path)
        sys.exit(0)

    harvester = Harverster(config_path=config_path, thumbnail=thumbnail, sample=sample, dump_metadata=dump_out)

    if reset:
        if input("You asked to reset the existing harvesting, this will removed all the already downloaded data files... are you sure? (y/n) ") == "y":
            harvester.reset()
        else:
            print("skipping reset...")

    start_time = time.time()

    harvester.harvest(csv_path)
    harvester.diagnostic()

    runtime = round(time.time() - start_time, 3)
    print("runtime: %s seconds " % (runtime))
