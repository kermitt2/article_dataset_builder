import json
import os

"""
Build a map of pii, doi, pmid, and local pdf file for the Elsevier COVID-19 open resources. 
To be executed at the root of the elsevier data resources after getting them on their sftp server.
The map is printed in the console. After gzip it, it is expected under article-dataset-builder/resources/

> python3 build_elsevier_map.py > elsevier_covid_map_23_03_2020.csv
> gzip elsevier_covid_map_23_03_2020.csv
> cp elsevier_covid_map_23_03_2020.csv.gz ~/article_dataset_builder/resources/

"""

if __name__ == "__main__":
    line = ",".join(["doi","pii","pmid","pdf"])
    print(line)
    for (dirpath, dirnames, filenames) in os.walk("./meta"):
        for filename in filenames:
            if filename.endswith('.json'): 
                the_file = os.sep.join([dirpath, filename])
            jsonfile = open(the_file, "r")
            fulljson = jsonfile.read().replace("\n", "")
            jsonfile.close()

            the_json = json.loads(fulljson)
            the_json = the_json["full-text-retrieval-response"]
            doi = ""
            if "prism:doi" in the_json["coredata"]:
                doi = the_json["coredata"]["prism:doi"]
            pii = the_json["coredata"]["pii"]
            pmid = ""
            if "pubmed-id" in the_json:
                pmid = the_json["pubmed-id"]
            pdf = filename.replace("_meta.json",".pdf")
            line = ",".join([doi,pii,pmid,pdf])
            print(line)
