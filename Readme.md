# Open Access PDF harvester and ingester

Python utility for harvesting efficiently a large Open Access collection of PDF and for transforming them into structured XML adapted to text mining and information retrieval applications.

## What:

- Harvester PDF from article set (basic metadata provided in a csv file)

- Perform some metadata enrichment/agregation via biblio-glutton and output consolidated metadata in a json file (default `consolidated_metadata.csv`)

- Perform [Grobid](https://github.com/kermitt2/grobid) full processing of PDF (including bibliographical reference consolidation and OA access resolution)

Optionally: 

- generate thumbnails for article (based on the first page of the PDF) 

- load the generated dataset on S3 instead of the local file system

- generate json PDF annotation (with coordinates) for inline reference markers and bibliographical references 

## Installation

The following tools need to be installed and running, with access information specified in the configuration file (`config.json`):

- [Grobid](https://github.com/kermitt2/grobid)

- [biblio-glutton](https://github.com/kermitt2/biblio-glutton)

It is possible to use public demo instances of these services, but the process will not be able to scale and won't be reliable given that the public servers are very frequently overloaded. 

As [biblio-glutton](https://github.com/kermitt2/biblio-glutton) is using dataset dump, there is a gap of several months in term of bibliographical data freshness. So, complementary, the web API services are used to cover the gap:

- Unpaywall API

- CrossRef web API

You need to indicate you email in the config file (`config.json`) to follow the etiquette policy of these services. 

## Usage

Fill the file `config.json` with relevant service and parameter url, then install the python mess:

> pip3 install -f requirements

For instance for the [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research), you can directly use the [source_metadata.csv](https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-13/all_sources_metadata_2020-03-13.csv) file by running: 

> python3 harvest --csv source_metadata.csv --out consolidated_metadata_out.json

This will generate a consolidated metadata file (specified by `--out`,  or `consolidated_metadata.json` by default), upload full text files, 
converted tei.xml files and other optional files either in the local file system (under data_path indicated in the config.json 
file) or on a S3 bucket if the fields are filled in config.json. 

for example:

> python3 harvest --csv all_sources_metadata_2020-03-13.csv     

you can set a specific config file name like this:

> python3 harvest --csv all_sources_metadata_2020-03-13.csv --config my_config_file.json    

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


