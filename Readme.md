# Open Access PDF harvester and ingester

Python utility for harvesting efficiently a large Open Access collection of PDF (fault tolerant, can be resumed, parallel download and ingestion) and for transforming them into structured XML adapted to text mining and information retrieval applications.

Currently supported:

- list of DOI in a file, one DOI per line
- metadata csv input file from [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research)

## What:

- Perform some metadata enrichment/agregation via biblio-glutton & CrossRef API and output consolidated metadata in a json file (default output `consolidated_metadata.csv`)

- Harvester PDF from article set (basic metadata provided in a csv file), e.g. typically available Open Access PDF via Unpaywall API (and some heuristics) 

- Perform [Grobid](https://github.com/kermitt2/grobid) full processing of PDF (including bibliographical reference consolidation and OA access resolution of the cited references)

Optionally: 

- generate thumbnails for article (based on the first page of the PDF) 

- upload the generated dataset on S3 instead of the local file system

- generate json PDF annotation (with coordinates) for inline reference markers and bibliographical references 

## Installation

The following tools need to be installed and running, with access information specified in the configuration file (`config.json`):

- [Grobid](https://github.com/kermitt2/grobid)

- [biblio-glutton](https://github.com/kermitt2/biblio-glutton)

It is possible to use public demo instances of these services, but the process will not be able to scale and won't be reliable given that the public servers are very frequently overloaded. 

As [biblio-glutton](https://github.com/kermitt2/biblio-glutton) is using dataset dumps, there is a gap of several months in term of bibliographical data freshness. So, complementary, the Crossref web API and Unpaywall API services are used to cover the gap:

- [Unpaywall API](https://unpaywall.org/products/api)

- [CrossRef web API](https://github.com/CrossRef/rest-api-doc)

You need to indicate your email in the config file (`config.json`) to follow the etiquette policy of these two services. 

An important parameter in the `config.json` file is the number of parallel document processing that is allowed, this is specified by the attribute `batch_size`, default value being `10` (so 10 documents max downloaded in parallel with distinct threads/workers and processed by Grobid in parallel). You can set this number according to your available number of threads.   

## Usage

```
usage: harvest.py [-h] [--dois DOIS] [--cord19 CORD19] [--config CONFIG]
                  [--reset] [--reprocess] [--thumbnail] [--annotation]
                  [--dump DUMP]

COVIDataset harvester

optional arguments:
  -h, --help       show this help message and exit
  --dois DOIS      path to a file describing a dataset articles as a simple
                   list of DOI (one per line)
  --cord19 CORD19  path to the csv file describing the CORD-19 dataset
                   articles
  --config CONFIG  path to the config file, default is ./config.json
  --reset          ignore previous processing states, and re-init the
                   harvesting process from the beginning
  --reprocess      reprocessed existing failed entries
  --thumbnail      generate thumbnail files for the front page of the
                   harvested PDF
  --annotation     generate bibliographical annotations with coordinates for
                   the harvested PDF
  --dump DUMP      write all the consolidated metadata in json
```

Fill the file `config.json` with relevant service and parameter url, then install the python mess:

> pip3 install -f requirements

For instance to process a list of DOI:

> python3 harvest --dois my_dois.txt 

For instance for the [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research), you can directly use the [source_metadata.csv](https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-13/all_sources_metadata_2020-03-13.csv) file by running: 

> python3 harvest --cord19 all_sources_metadata_2020-03-13.csv  

This will generate a consolidated metadata file (specified by `--out`,  or `consolidated_metadata.json` by default), upload full text files, 
converted tei.xml files and other optional files either in the local file system (under data_path indicated in the config.json 
file) or on a S3 bucket if the fields are filled in config.json. 

You can set a specific config file name like this:

> python3 harvest --cord19 all_sources_metadata_2020-03-13.csv --config my_config_file.json    

To resume an interrupted processing, simply re-run the same command. 

To reprocess the failed articles of an harvesting, use:

> python3 harvest.py --reprocess 

To create a dump of the consolidated metadata of all the processed files (including the UUID identifier and the state of processing):

> python3 harvest.py --dump consolidated_metadata.json


## Generated files

Structure of the generated files for an article having as UUID identifier `98da17ff-bf7e-4d43-bdf2-4d8d831481e5`

```
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.pdf
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.tei.xml
```

Optional additional files:

```
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-ref-annotations.json
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-small.png
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-medium.png
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-large.png
```

The UUID identifier for a particular article is given in the generated `consolidated_metadata.csv` file.


