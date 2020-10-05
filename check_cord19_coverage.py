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

def check_coverage(metadata, documents):
    # cord_uid,sha,source_x,title,doi,pmcid,pubmed_id,license,abstract,publish_time,authors,journal,Microsoft Academic Paper ID,
    # WHO #Covidence,has_full_text,full_text_file,url
    line_count = 0 # total count of articles
    at_least_one = 0; # total of entries with at least one json
    json_pdf = 0; # total of entries with a PDF-derived json
    json_pmc = 0; # total of entries with a PMC-NLM-derived json

    # counting lines for the progress bar
    nb_lines = 0
    with open(metadata, mode='r') as csv_file:
        for line in csv_file:
            nb_lines += 1

    pbar = tqdm(total = nb_lines)
    with open(metadata, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        
        # there are double cord id, so we need to keep track of them
        cord_ids = []

        for row in csv_reader:
            if line_count % 100 == 0:
                pbar.update(100)

            # append the headers and values from checks in add_to_row
            cord_id = row["cord_uid"]
            if cord_id in cord_ids:
                continue
            cord_ids.append(cord_id)

            pdf_sha = row["sha"]
            pmc_id = row["pmcid"]
            json_present = False

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
   
            line_count += 1
            if json_present:
                at_least_one += 1


        print("/nprocessed", str(line_count), "articles from CORD-19")
        print("total distinct cord id:", len(cord_ids))

    pbar.close()

    print("total PMC-derived JSON:", json_pmc)
    print("total PDF-derived JSON:", json_pdf)
    print("total entry with at least one JSON:", at_least_one)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "COVIDataset harvester")
    parser.add_argument("--documents", help="path to the CORD-19 uncompressed document dataset") 
    parser.add_argument("--metadata", help="path to the CORD-19 CSV metadata file") 
    args = parser.parse_args()
    documents_path = args.documents
    metadata_path = args.metadata
    
    check_coverage(metadata_path, documents_path)