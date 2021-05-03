"""
Simple script to count the full texts available in the official CORD-19 dataset.
We have json "fulltext" from pdf (using sha for file name) and json from PMC NLM 
(using the PMC number as file name). So we need to convert these name files to 
CORD id to see the actual avaibility of full text in the dataset.
"""

import argparse
import os
import shutil
import json
import time
import csv
from tqdm import tqdm
from harvest import generateStoragePath

def check_coverage(config_path, metadata, documents):
    config = _load_config(config_path)
    data_path = config["data_path"]

    # cord_uid,sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,Microsoft Academic Paper ID,
    # WHO #Covidence,has_full_text,full_text_file,url
    line_count = 0 # total count of articles
    at_least_one = 0 # total of entries with at least one json
    at_least_one_tei = 0 # total of entries with at least one TEI in our harvesting
    json_pdf = 0 # total of entries with a PDF-derived json
    json_pmc = 0 # total of entries with a PMC-NLM-derived json
    harvested = 0 # total of entries with PDF-derived or PMC-NLM-derived json which has been harvested
    harvested_grobid = 0 # total of entries with PDF-derived json which has been harvested
    harvested_pmc = 0 # total of entries with PMC-NLM-derived json which has been harvested
    extra_harvested = 0 # total of entries harvested but no full text in CORD-19
    extra_harvested_grobid = 0 # total of entries harvested via grobid but no full text in CORD-19
    extra_harvested_pmc = 0 # total of entries harvested via PMC-NLM but no full text in CORD-19

    # counting lines for the progress bar
    nb_lines = 0
    header_line = None # for storing the header line
    with open(metadata, mode='r') as csv_file:
        for line in csv_file:
            if nb_lines == 0:
                header_line = line
            nb_lines += 1

    path_missed = os.path.join(data_path,"missed_entries.csv")
    path_extra = os.path.join(data_path,"extra_entries.csv")

    pbar = tqdm(total = nb_lines)
    with open(path_missed,'w') as file_missed:
        with open(path_extra,'w') as file_extra:
            header_line = 'cord_uid'
            file_missed.write(header_line+',url\n')
            header_line += 'doi,pmc,pmid,path_grobid,path_pub2tei\n'
            file_extra.write(header_line)
            with open(metadata, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                
                # there are double cord id, so we need to keep track of them
                cord_ids = []
                for row in csv_reader:
                    line_count += 1
                    if line_count % 100 == 0:
                        pbar.update(100)

                    # append the headers and values from checks in add_to_row
                    cord_id = row["cord_uid"]
                    if cord_id in cord_ids:
                        continue
                    cord_ids.append(cord_id)

                    pdf_sha = row["sha"]
                    pmc_id = row["pmcid"]
                    doi = row["doi"]
                    pmid = row["pubmed_id"]
                    json_present = False
                    extra = False

                    # check PMC-derived json 
                    pmc_json = os.path.join(documents, "document_parses", "pmc_json", pmc_id+".xml.json")
                    if os.path.exists(pmc_json) and os.path.isfile(pmc_json):
                        json_pmc += 1
                        json_present = True

                    # check PDF-derived json
                    pdf_json = os.path.join(documents, "document_parses", "pdf_json", pdf_sha+".json")
                    if os.path.exists(pdf_json) and os.path.isfile(pdf_json):
                        json_pdf+= 1
                        json_present = True
           
                    if json_present:
                        at_least_one += 1

                    # do we have the document in the local harvested data ?
                    dest_path = generateStoragePath(cord_id)
                    local_doc_path_grobid = os.path.join(data_path, dest_path, cord_id+".grobid.tei.xml")
                    local_doc_path_pub2tei = os.path.join(data_path, dest_path, cord_id+".pub2tei.tei.xml")
                    if (os.path.exists(local_doc_path_grobid) and os.path.isfile(local_doc_path_grobid)) or (os.path.exists(local_doc_path_pub2tei) and os.path.isfile(local_doc_path_pub2tei)):
                        at_least_one_tei += 1
                        if json_present:
                            harvested += 1
                        else:
                            extra_harvested += 1
                    elif json_present:
                        file_missed.write(cord_id+","+row["url"]+"\n")
                        extra = False

                    if os.path.exists(local_doc_path_grobid) and os.path.isfile(local_doc_path_grobid):
                        if json_present:
                            harvested_grobid += 1
                        else:
                            extra_harvested_grobid += 1
                            extra = True

                    if os.path.exists(local_doc_path_pub2tei) and os.path.isfile(local_doc_path_pub2tei):
                        if json_present:
                            harvested_pmc += 1
                        else:
                            extra_harvested_pmc += 1
                            extra = True

                    if extra: 
                        line = cord_id + "," + doi + "," + pmc_id + "," + pmid + ","
                        if os.path.exists(local_doc_path_grobid) and os.path.isfile(local_doc_path_grobid):
                            line += local_doc_path_grobid
                        line += ","
                        if os.path.exists(local_doc_path_pub2tei) and os.path.isfile(local_doc_path_pub2tei):
                            line += local_doc_path_pub2tei
                        line += "\n"
                        file_extra.write(line)

    pbar.close()

    print("\nprocessed", str(line_count), "article entries from CORD-19 metadata file")
    print("total distinct cord id with JSON full text:", str(len(cord_ids)), "("+str(line_count - len(cord_ids)),"duplicated cord ids)")

    print("total CORD-19 official PMC-derived JSON:", json_pmc)
    print("total CORD-19 official PDF-derived JSON:", json_pdf)
    print("total entry with at least one JSON (official CORD-19):", at_least_one)

    print("\ntotal CORD-19 JSON we harvested too:", harvested)
    print("\tvia Unpaywall PDF and GROBID:", harvested_grobid)
    print("\tvia PMC/NLM and Pub2TEI:", harvested_pmc)
    print("\t -> we missed", str(at_least_one-harvested), "entries, see file", path_missed, "for the list missed entries")

    print("\ntotal extra fultext we harvested in addition:", extra_harvested)
    print("\tvia Unpaywall PDF and GROBID:", extra_harvested_grobid)
    print("\tvia PMC/NLM and Pub2TEI:", extra_harvested_pmc)
    print("\t -> see file", path_extra,"for the list of extra entries")

    print("\ntotal distinct cord id with TEI XML full text (our harvesting):", at_least_one_tei)

def _load_config(config_path):
    """
    Load the json configuration 
    """
    config_json = open(config_path).read()
    return json.loads(config_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "COVIDataset harvester")
    parser.add_argument("--documents", help="path to the official CORD-19 uncompressed document dataset") 
    parser.add_argument("--metadata", help="path to the CORD-19 CSV metadata file") 
    parser.add_argument("--config", default="./config.json", help="path to the config file, default is ./config.json") 
    args = parser.parse_args()
    config_path = args.config
    documents_path = args.documents
    metadata_path = args.metadata
    
    check_coverage(config_path, metadata_path, documents_path)
