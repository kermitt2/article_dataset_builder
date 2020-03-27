# Open Access PDF harvester and ingester

Python utility for harvesting efficiently a large Open Access collection of PDF (fault tolerant, can be resumed, parallel download and ingestion) and for transforming them into structured XML adapted to text mining and information retrieval applications.

Input currently supported:

- list of DOI in a file, one DOI per line
- metadata csv input file from [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research)
- list of PMID in a file, one DOI per line
- list of PMC ID in a file, one DOI per line

To do:
- list of ISTEX identifiers or ark, one DOI per line

## What:

- Perform some metadata enrichment/agregation via [biblio-glutton](https://github.com/kermitt2/biblio-glutton) & [CrossRef web API](https://github.com/CrossRef/rest-api-doc) and output consolidated metadata in a json file 

- Harvest PDF from the specification of the article set (list of strong identifiers or basic metadata provided in a csv file), typically PDF available in Open Access PDF via the [Unpaywall API](https://unpaywall.org/products/api) (and some heuristics) 

- Perform [Grobid](https://github.com/kermitt2/grobid) full processing of PDF (including bibliographical reference consolidation and OA access resolution of the cited references), converting them into structured XML TEI

- For PMC files (Open Access set only), harvest also XML JATS (NLM) files and perform a conversion into XML TEI (same TEI customization as Grobid) via [Pub2TEI](https://github.com/kermitt2/Pub2TEI)

Optionally: 

- Generate thumbnails for article (based on the first page of the PDF), small/medium/large 

- Upload the generated dataset on S3 instead of the local file system

- Generate json PDF annotations (with coordinates) for inline reference markers and bibliographical references (see [here](https://grobid.readthedocs.io/en/latest/Grobid-service/#apireferenceannotations))

## Requirements

The utility has been tested with Python 3.5+. It is developed for a deployment on a POSIX/Linux server (it uses `imagemagick` as external process to generate thumbnails and `wget`). An S3 account and bucket must have been created for non-local storage of the data collection. 


## Installation

The following tools need to be installed and running, with access information specified in the configuration file (`config.json`):

- [Grobid](https://github.com/kermitt2/grobid), for converting PDF into XML TEI

- [biblio-glutton](https://github.com/kermitt2/biblio-glutton), for metadata retrieval and aggregation

- [Pub2TEI](https://github.com/kermitt2/Pub2TEI), for converting PMC XML files into XML TEI

It should be possible to use the public demo instance of [biblio-glutton](https://github.com/kermitt2/biblio-glutton), as default configured in the `config.json` file (the tool scale at more than 6000 queries per second). However for [Grobid](https://github.com/kermitt2/grobid), we strongly recommand to install a local instance, because the online public demo will not be able to scale and won't be reliable given that it is more or less always overloaded. 

As [biblio-glutton](https://github.com/kermitt2/biblio-glutton) is using dataset dumps, there is a gap of several months in term of bibliographical data freshness. So, complementary, the [CrossRef web API](https://github.com/CrossRef/rest-api-doc) and [Unpaywall API](https://unpaywall.org/products/api) services are used to cover the gap. For these two services, you need to indicate your email in the config file (`config.json`) to follow the etiquette policy of these two services. 

An important parameter in the `config.json` file is the number of parallel document processing that is allowed, this is specified by the attribute `batch_size`, default value being `10` (so 10 documents max downloaded in parallel with distinct threads/workers and processed by Grobid in parallel). You can set this number according to your available number of threads.   

These tools requires Java 8 or more. 

## Docker

TBD

## Usage

```
usage: harvest.py [-h] [--dois DOIS] [--cord19 CORD19] [--pmids PMIDS]
                  [--pmcids PMCIDS] [--config CONFIG] [--reset] [--reprocess]
                  [--thumbnail] [--annotation] [--dump]

COVIDataset harvester

optional arguments:
  -h, --help       show this help message and exit
  --dois DOIS      path to a file describing a dataset articles as a simple
                   list of DOI (one per line)
  --cord19 CORD19  path to the csv file describing the CORD-19 dataset
                   articles
  --pmids PMIDS    path to a file describing a dataset articles as a simple
                   list of PMID (one per line)
  --pmcids PMCIDS  path to a file describing a dataset articles as a simple
                   list of PMC ID (one per line)
  --config CONFIG  path to the config file, default is ./config.json
  --reset          ignore previous processing states, and re-init the
                   harvesting process from the beginning
  --reprocess      reprocessed existing failed entries
  --thumbnail      generate thumbnail files for the front page of the
                   harvested PDF
  --annotation     generate bibliographical annotations with coordinates for
                   the harvested PDF
  --dump           write all the consolidated metadata in json in the file
                   consolidated_metadata.json
```

Fill the file `config.json` with relevant service and parameter url, then install the python mess:

```console
pip3 install -f requirements
```

For instance to process a list of DOI (one DOI per line):

```console
python3 harvest --dois test/dois.txt 
```

Similarly for a list of PMID or PMC ID:

```console
python3 harvest --dois test/pmids.txt 
python3 harvest --dois test/pmcids.txt 
```

For instance for the [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research), you can use the [metadata.csv](https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-20/metadata.csv) file by running: 

```console
python3 harvest --cord19 metadata.csv  
```

This will generate a consolidated metadata file (specified by `--out`,  or `consolidated_metadata.json` by default), upload full text files, 
converted tei.xml files and other optional files either in the local file system (under data_path indicated in the config.json 
file) or on a S3 bucket if the fields are filled in config.json. 

You can set a specific config file name with `--config` :

```console
python3 harvest --cord19 metadata.csv --config my_config_file.json    
```

To resume an interrupted processing, simply re-run the same command. 

To re-process the failed articles of an harvesting, use:

```console
python3 harvest.py --reprocess 
```

To reset entirely an existing harvesting and re-start an harvesting from zero:

```console
python3 harvest.py --cord19 metadata.csv --reset
```

To create a dump of the consolidated metadata of all the processed files (including the UUID identifier and the state of processing), add the parameter `--dump`:

```console
python3 harvest.py --dump 
```

The generated file is named `consolidated_metadata.json`.

## Generated files

Structure of the generated files for an article having as UUID identifier `98da17ff-bf7e-4d43-bdf2-4d8d831481e5`

```
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.pdf
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.grobid.tei.xml
```

Optional additional files:

```
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.nxml
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-ref-annotations.json
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-small.png
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-medium.png
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-large.png
```

The UUID identifier for a particular article is given in the generated `consolidated_metadata.csv` file.

The `*.nxml` files correspond to the JATS files available for PMC (Open Access set only).

## Using a local PDF repository for CORD-19

The [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research) includes more than 19k articles corresponding to a set of Elsevier articles on COVID-19 [recently put in Open Access](https://www.elsevier.com/connect/coronavirus-information-center). As Unpaywall does not cover these OA articles (on 23.03.2020 at least), you would need to download first these PDF and indicates to the harvesting tool where the local repository of PDF is located: 

- download the PDF files on the COVID-19 FTP server: 

```console
sftp public@coronacontent.np.elsst.com
```

Indicate `beat_corona` as password. See the [instruction page](https://www.elsevier.com/connect/coronavirus-information-center#researchers) in case of troubles. 

```console
cd pdf
mget *
```

- indicate the local repository where you have downloaded the dataset in the `config.json` file:

```json
"cord19_elsevier_pdf_path": "/the/path/to/the/pdf"
```

That's it. The file `./elsevier_covid_map_23_03_2020.csv.gz` contains a map of DOI and PII (the Elsevier article identifiers) for these OA articles. 

## Converting the PMC XML JATS files into XML TEI

After the harvesting and processing realised by `harvest.py`, it is possible to convert of PMC XML JATS files into XML TEI. This will provide better XML quality than what can be extracted automatically by Grobid from the PDF. This conversion allows to have all the documents in the same XML TEI customization format. As the TEI format superseeds JATS, there is normally no loss of information from the JATS file.  

To launch the conversion:


```console
python3 nlm2tei.py
```

If a custom config file is used:

```console
python3 nlm2tei.py --config ./my_config.json
```

This will apply Pub2TEI (a set of XSLT) to all the harvested `*.nxml` files and add to the document repository a new file TEI file:

```
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.pub2tei.tei.xml
```

Note that Pub2TEI supports a lot of other publisher's XML formats, so the principle could be extended to transform a lot of different XML formats into a single one (TEI), facilitating further ingestion and process by avoiding to write complicated XML parsers for each case. 

## Troubleshooting with imagemagick

Recent update (end of October 2018) of imagemagick is breaking the normal conversion usage. Basically the converter does not convert by default for security reason related to server usage. For non-server mode as involved in our module, it is not a problem to allow PDF conversion. For this, simply edit the file `/etc/ImageMagick-6/policy.xml` (or `/etc/ImageMagick/policy.xml`) and put into comment the following line: 

```
<!-- <policy domain="coder" rights="none" pattern="PDF" /> -->
```

## License and contact

Distributed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0). The dependencies used in the project are either themselves also distributed under Apache 2.0 license or distributed under a compatible license. 

Main author and contact: Patrice Lopez (<patrice.lopez@science-miner.com>)
