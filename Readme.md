[![PyPI version](https://badge.fury.io/py/article_dataset_builder.svg)](https://badge.fury.io/py/article_dataset_builder)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# Open Access scholar PDF harvester and ingester

Python utility for harvesting efficiently a large Open Access collection of scholar PDF (fault tolerant, can be resumed, parallel download and ingestion) and for transforming them into structured XML adapted to text mining and information retrieval applications.

Input currently supported:

- list of DOI in a file, one DOI per line
- list of PMID in a file, one DOI per line
- list of PMC ID in a file, one DOI per line
- metadata csv input file from [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research), see the CORD-19 result section below to see the capacity of the tool to get more full texts and better data quality that the official dataset

The harvesting is following fair-use (which means that it covers non re-sharable articles) and it is exploiting various Open Access sources. The harvesting thus should result in a close-to-optimal discovery of OA full texts. For instance, from the same CORD-19 metadata file, the tool can harvest 35.5% more usable full text than available in the CORD-19 dataset (140,322 articles with at least one usable full text versus 103,587 articles with at least one usable full text for the CORD-19 dataset version 2020-09-11), see statistics [here](https://github.com/kermitt2/article_dataset_builder#results-with-cord-19). 

To do:
- Apache Airflow for more complex task workflow and automated periodic incremental harvesting
- Consolidate/resolve bibliographical references obtained via Pub2TEI
- Upload harvested resources on SWIFT object storage (OpenStack) in addition to local and S3 storage

## What this doing is doing

- Perform some metadata enrichment/agregation via [biblio-glutton](https://github.com/kermitt2/biblio-glutton) & [CrossRef web API](https://github.com/CrossRef/rest-api-doc) and output consolidated metadata in a json file 

- Harvest PDF from the specification of the article set (list of strong identifiers or basic metadata provided in a csv file), typically PDF available in Open Access PDF via the [Unpaywall API](https://unpaywall.org/products/api) and some heuristics

- Optionally, perform [Grobid](https://github.com/kermitt2/grobid) full processing of PDF (including bibliographical reference consolidation and OA access resolution of the cited references), converting them into structured XML TEI

- For PMC files (Open Access set only), harvest also XML JATS (NLM) files and perform optionally a conversion into XML TEI (same TEI customization as Grobid) via [Pub2TEI](https://github.com/kermitt2/Pub2TEI)

In addition, optionally: 

- Generate thumbnails for article (based on the first page of the PDF), small/medium/large 

- Upload the generated dataset on S3, instead of the local file system

- Generate json PDF annotations (with coordinates) for inline reference markers and bibliographical references (see [here](https://grobid.readthedocs.io/en/latest/Grobid-service/#apireferenceannotations))

## Requirements

The utility has been tested with Python 3.5+. It is developed for a deployment on a POSIX/Linux server (it uses `imagemagick` as external process to generate thumbnails and `wget`). An S3 account and bucket must have been created for non-local storage of the data collection. 

If you wish to generate thumbnails for article as they are harvested, install `imagemagick`:

- on Linux Ubuntu:

```console
sudo apt update
sudo apt build-dep imagemagick
```

- on macos:

```console
brew install libmagic
```

## Installation

### Article dataset builder

#### Local install

For installing the tool locally and use the current master version, get the github repo:

```sh
git clone https://github.com/kermitt2/article_dataset_builder
cd delft
```

Create a virtual environment and install the Python mess:

```console
virtualenv --system-site-packages -p python3.8 env
source env/bin/activate
python3 -m pip install -r requirements.txt
```

Finally install the project, preferably in editable state

```sh
python3 -m pip  install -e .
```

#### Using PyPI package

PyPI packages are available for stable versions. Latest stable version is normally `0.2.4`, but double check [here](https://pypi.org/project/article-dataset-builder/):

```
python3 -m pip install article-dataset-builder==0.2.4
```

### Third party web services

Depending on the process you are interested to apply to the articles as they are harvested, the following tools need to be installed/accessed and running, with access information specified in the configuration file (`config.json`):

- [biblio-glutton](https://github.com/kermitt2/biblio-glutton), for metadata retrieval and aggregation

It should be possible to use the public demo instance of [biblio-glutton](https://github.com/kermitt2/biblio-glutton), as default configured in the `config.json` file (the tool scale at more than 6000 queries per second). However in combaination with [Grobid](https://github.com/kermitt2/grobid), we strongly recommand to install a local instance, because the online public demo will not be able to scale and won't be reliable given that it is more or less always overloaded. 

- [Grobid](https://github.com/kermitt2/grobid), for converting PDF into XML TEI

This tool requires Java 8 to 11. 

- [Pub2TEI](https://github.com/kermitt2/Pub2TEI), for converting PMC XML files into XML TEI

As [biblio-glutton](https://github.com/kermitt2/biblio-glutton) is using dataset dumps, there is a gap of several months in term of bibliographical data freshness. So, complementary, the [CrossRef web API](https://github.com/CrossRef/rest-api-doc) and [Unpaywall API](https://unpaywall.org/products/api) services are used to cover the gap. For these two services, you need to indicate your email in the config file (`config.json`) to follow the etiquette policy of these two services. If the configuration parameters for `biblio-glutton` are empty, only the CrossRef REST API will be used. 

An important parameter in the `config.json` file is the number of parallel document processing that is allowed, this is specified by the attribute `batch_size`, default value being `10` (so 10 documents max downloaded in parallel with distinct threads/workers and processed by Grobid in parallel). You can set this number according to your available number of threads. If you do not apply Grobid on the downloaded PDF, you can raise the `batch_size` parameter significantly, for example to `50`, which means then 100 paralell download. Be careful that parallel download from the same source might be blocked or might result in black-listing for some OA publisher sites, so it might be better to keep `batch_size` reasonable even when only donwloading.  

For downloading preferably the fulltexts available at PubMed Central from the NIH site (PDF and JATS XML files) rather than on publisher sites, the Open Access list file from PMC that maps PMC identifiers to PMC resource archive URL will be downloaded automatically. You can also download it manually as follow:

```console
cd resources
wget https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt
```

When this file is available under `resources/oa_file_list.txt`, an index will be built at first launch and the harvester will prioritize the access to the NIH resources. 

## Docker

TBD

## Usage

```
python3 article_dataset_builder/harvest.py --help
usage: harvest.py [-h] [--dois DOIS] [--cord19 CORD19] [--pmids PMIDS] [--pmcids PMCIDS]
                  [--config CONFIG] [--reset] [--reprocess] [--thumbnail] [--annotation]
                  [--diagnostic] [--dump] [--grobid]
Scholar PDF harvester and converter

optional arguments:
  -h, --help       show this help message and exit
  --dois DOIS      path to a file describing a dataset articles as a simple list of DOI (one per
                   line)
  --cord19 CORD19  path to the csv file describing the CORD-19 dataset articles
  --pmids PMIDS    path to a file describing a dataset articles as a simple list of PMID (one
                   per line)
  --pmcids PMCIDS  path to a file describing a dataset articles as a simple list of PMC ID (one
                   per line)
  --config CONFIG  path to the config file, default is ./config.json
  --reset          ignore previous processing states, and re-init the harvesting process from
                   the beginning
  --reprocess      reprocessed existing failed entries
  --thumbnail      generate thumbnail files for the front page of the harvested PDF
  --annotation     generate bibliographical annotations with coordinates for the harvested PDF
  --diagnostic     perform a full consistency diagnostic on the harvesting and transformation
                   process
  --dump           write all the consolidated metadata in json in the file
                   consolidated_metadata.json
  --grobid         process downloaded files with Grobid to generate full text XML
```

Fill the file `config.json` with relevant service and parameter url.

For example to harvest a list of DOI (one DOI per line):

```console
python3 article_dataset_builder/harvest.py --dois test/dois.txt 
```

Similarly for a list of PMID or PMC ID with Grobid conversion of the PDF as the are downloaded:

```console
python3 article_dataset_builder/harvest.py --pmids test/pmids.txt --grobid
python3 article_dataset_builder/harvest.py --pmcids test/pmcids.txt --grobid
```

For example for the [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research), you can use the [metadata.csv](https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases.html) (last tested version from 2020-06-29) file by running: 

```console
python3 harvest.py --cord19 metadata.csv  
```

An harvesting will generate a JSONL mapping file with the UUID associated with to the Open Access full text resource and the main identifiers of the entries under the data path specified in the configuration file and named `map.json`. The harvesting uploads full text files, converted tei.xml files and other optional files either in the local file system (under data_path indicated in the `config.json` file) or on a S3 bucket if the fields are filled in `config.json`. 

You can set a specific config file name with `--config` :

```console
python3 harvest.py --cord19 metadata.csv --config my_config.json    
```

To resume an interrupted processing, **simply re-run the same command**. 

To re-process the failed articles of an harvesting, use:

```console
python3 harvest.py --reprocess --config my_config.json  
```

To reset entirely an existing harvesting and re-start an harvesting from zero (it means all the already harvested PDF will be deleted!): 

```console
python3 harvest.py --cord19 metadata.csv --reset --config my_config.json  
```

To download full texts (PDF and JATS/NLM) with GROBID processing, use `--grobid` parameter:

```console
python3 harvest.py --cord19 metadata.csv --config my_config.json --grobid
```

To create a full dump of the consolidated metadata for all the scholar articles (including the UUID identifier and the state of processing), add the parameter `--dump`:

```console
python3 harvest.py --dump --config my_config.json  
```

The generated metadata file is named `consolidated_metadata.json`.

For producing the thumbnail images of the article first page, use `--thumbnail` argument. This option requires `imagemagick` installed on your system and will produce 3 PNG files of size height x150, x300 and x500. These thumbnails can be interesting for offering a preview to an article for an application using these data.

```console
python3 harvest.py --cord19 metadata.csv --thumbnail --config my_config.json  
```

For producing PDF annotations in JSON format corresponding to the bibliographical information (reference markers in the article and bibliographical references in the bibliographical section), use the argument `--annotation`. See more information about these annotations [here](https://grobid.readthedocs.io/en/latest/Coordinates-in-PDF/). They allow to enrich the display of PDF, and make them more interactive. 

```console
python3 harvest.py --cord19 metadata.csv --annotation --config my_config.json  
```

Finally you can run a short diagnostic/reporting on the latest harvesting by adding `--diagnostic` (combined to any previous harvesting command line, or just as unique parameter, to make a diagnostic to the realized harvesting):

```console
python3 harvest.py --diagnostic --config my_config.json  
```

## Generated files

### Default 

Structure of the generated files for an article having as UUID identifier `98da17ff-bf7e-4d43-bdf2-4d8d831481e5`

```
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.pdf
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.json
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.grobid.tei.xml
```

The `*.json` file above gives the consolidated metadata of the harvested item, based on CrossRef entries, with additional information provided by `biblio-glutton`, status of the harvesting and GROBID processing and UUID (field `id`). 

Optional additional files:

```
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.nxml
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5.pub2tei.tei.xml
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-ref-annotations.json
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-small.png
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-medium.png
98/da/17/ff/98da17ff-bf7e-4d43-bdf2-4d8d831481e5/98da17ff-bf7e-4d43-bdf2-4d8d831481e5-thumb-large.png
```

File with extension `*.nxml` are JATS XML files downloaded from PubMed Central (Open Access set only). File with extension `*.pub2tei.tei.xml` are converted JATS file into TEI XML (same XML full text format as Grobid TEI) with Pub2TEI. File with suffix `*-ref-annotations.json` are JSON PDF coordinates of structures recognized by GROBID. Files with extension `*.png` are thumbnail images of the first page of the harvested PDF.

The UUID identifier for a particular article is given in the generated `map.json` file under the data path, associated to the every document identifiers. The UUID is also given in the `consolidated_metadata.csv` file (obtained with option `--dump`, see above).

## CORD-19 example

The tool can realize its own harvesting and ingestion of CORD-19 papers based on an official version of the `metadata.csv` file of CORD-19. It provides two main advantages as compared to the official CORD-19 dataset:

- Harvest around 35% more usable full texts for the CORD-19 `2020-09-11` version, around 25% more for the CORD-19 `2021-07-26` version
- Structure the full texts into high quality TEI format (both from PDF and from JATS), with much more information than the JSON format of the CORD-19 dataset. The JATS conversion into TEI in particular does not lose any information from the original XML file.

Be sure to install the latest available version of GROBID, many improvements have been added to the tool regarding the support of bioRxiv and medRxiv preprints.

To launch the harvesting (see above for more details):

```console
python3 harvest.py --cord19 metadata.csv --grobid
```

For the CORD-19 dataset, for simplification and clarity, we reuse the `cord id` which is a random string 8 characters in `[0-9a-z]`: 

```
00/0a/je/vz/000ajevz/000ajevz.pdf
00/0a/je/vz/000ajevz/000ajevz.json
00/0a/je/vz/000ajevz/000ajevz.grobid.tei.xml
```

Optional additional files:

```
00/0a/je/vz/000ajevz/000ajevz.nxml
00/0a/je/vz/000ajevz/000ajevz.pub2tei.tei.xml
00/0a/je/vz/000ajevz/000ajevz-ref-annotations.json
00/0a/je/vz/000ajevz/000ajevz-thumb-small.png
00/0a/je/vz/000ajevz/000ajevz-thumb-medium.png
00/0a/je/vz/000ajevz/000ajevz-thumb-large.png
```

For harvesting and structuring, you only need the metadata file of the CORD-19 dataset, available at: 

  https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/<date_iso_str>/metadata.csv

where `<date_iso_str>` should match a release date indicated on the [CORD-19 release page](https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases.html).

For running the coverage script, which compares the full text coverage of the official CORD-19 dataset with the one produced by the present tool, you will need the full CORD-19 dataset. 

## On harvesting and ingesting the CORD-19 dataset

### Adding a local PDF repository for Elsevier OA COVID papers

The [CORD-19 dataset](https://pages.semanticscholar.org/coronavirus-research) includes more than 19k articles corresponding to a set of Elsevier articles on COVID-19 [recently put in Open Access](https://www.elsevier.com/connect/coronavirus-information-center). As Unpaywall does not cover these OA articles (on 2020-03-23 at least), you will need to download first these PDF and indicates to the harvesting tool where the local repository of PDF is located: 

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

That's it. The file `./elsevier_covid_map_28_06_2020.csv.gz` contains a map of DOI and PII (the Elsevier article identifiers) for these OA articles. 

### Incremental harvesting

CORD-19 is updated regularly. Suppose that you have harvested one release of the CORD-19 full texts and a few weeks later you would like to refresh your local corpus. Incremental harvesting is supported, so only the new entries will be uploaded and ingested. 

If the harvesting was done with one version of the metadata file `metadata-2020-09-11.csv` (from the `2020-09-11` release):

```console
python3 article_dataset_builder/harvest.py --cord19 metadata-2020-09-11.csv --config my_config.json --grobid
```

The incremental update will be realized with a new version of the metadata file simply by specifying it:

```console
python3 article_dataset_builder/harvest.py --cord19 metadata-2021-03-22.csv --config my_config.json --grobid
```

The constraint is that the same data repository path is kept in the config file. The repository and its state will be reused to check if an entry has already been harvested or not.

As an alternative, it is also possible to point to a local old data directory in the config file, with parameter `legacy_data_path`. Before trying to download a file from the internet, the harvester will first check in this older data directory if the PDF files are not already locally available based on the same identifiers. 

### Results with CORD-19

Here are the results regarding the CORD-19 from __2020-09-11__ ([cord-19_2020-09-11.tar.gz](https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases/cord-19_2020-09-11.tar.gz)) (4.6GB) to illustrate the interest of this harvester tool. We used the present tool using the CORD-19 metadata file (`metadata.csv`), re-harvested the full texts and converted all into the same target TEI XML format (without information loss with respect to the available publisher XML and GROBID PDF-to-XML conversion). 

|   | official CORD-19 | this harvester |
|---|---|---|
| total entries | 253,454 | 253,454 | 
| without `cord id` duplicates | 241,335 | 241,335 |
| without duplicates | - | 161,839 |
| entries with valid OA URL | - | 141,142 |
| entries with successfully downloaded PDF | - | 139,565 | 
| entries with structured full texts via GROBID | 94,541 (PDF JSON) | 138,440 (TEI XML) |
| entries with structured full texts via PMC JATS | 77,115 (PMC JSON) | 104,288 (TEI XML) |
| __total entries with at least one structured full text__ | __103,587 (PDF JSON or PMC JSON)__ | __140,322 (TEI XML)__ |

Other information for this harvester: 

- total OA URL not found or invalid: 20,697 (out of the 161,839 distinct articles)
- 760 GROBID PDF to TEI XML conversion failures (the average failure on random downloaded scholar PDF is normally around 1%, so we are at 0.5% and this is good)
- 45 Pub2TEI tranformations (NLM (JATS) -> TEI XML) reported as containing some kind of failure 

Other main differences include:

- the XML TEI contain richer structured full text (section titles, notes, formulas, etc.), 
- usage of up-to-date GROBID models for PDF conversion (with extra medRxiv and bioRxiv training data), 
- PMC JATS files conversion with [Pub2TEI](https://github.com/kermitt2/Pub2TEI) (normally without information loss because the TEI custumization we are using superseeds the structures covered by JATS). Note that a conversion from PMC JATS files has been introduced in CORD-19 from version 6. 
- full consolidation of the bibliographical references with publisher metadata, DOI, PMID, PMC ID, etc. when available (if you are into citation graphs)
- consolidation of article metadata with CrossRef and PubMed aggregations for the entries 
- optional coordinates of structures on the original PDF
- optional thumbnails for article preview

Some [notes on CORD-19 harvesting](notes-cord19.md), including numbers for update version `2021-07-26`.

## Converting the PMC XML JATS files into XML TEI

After the harvesting and processing realised by `article_dataset_builder/harvest.py`, it is possible to convert of PMC XML JATS files into XML TEI. This will provide better XML quality than what can be extracted automatically by Grobid from the PDF. This conversion allows to have all the documents in the same XML TEI customization format. As the TEI format superseeds JATS, there is no loss of information from the JATS file. It requires [Pub2TEI](https://github.com/kermitt2/Pub2TEI) to be installed and the path to Pub2TEI `pub2tei_path` to be set in the `config.json` file of the `article_dataset_builder` project.

To launch the conversion under the default `data/` directory:

```console
python3 article_dataset_builder/nlm2tei.py
```

If a custom config file and custom `data/` path are used:

```console
python3 article_dataset_builder/nlm2tei.py --config ./my_config.json
```

This will apply Pub2TEI (a set of XSLT) to all the harvested `*.nxml` files and add to the document repository a new file TEI file, for instance for a CORD-19 entry:

```
00/0a/je/vz/000ajevz/000ajevz.pub2tei.tei.xml
```

Note that Pub2TEI supports a lot of other publisher's XML formats (and variants of these formats), so the principle and current tool could be used to transform different publisher XML formats into a single one (TEI) - not just NLM/JATS, facilitating and centralizing further ingestion and process by avoiding to write complicated XML parsers for each case. 

## Checking CORD-19 dataset coverage

The following script checks the number of duplicated `cord id` (also done by the normal harvester), but also count the number of articles with at least one JSON full text file:


```
usage: article_dataset_builder/check_cord19_coverage.py [-h] [--documents DOCUMENTS]
                                [--metadata METADATA]

COVIDataset harvester

optional arguments:
  -h, --help            show this help message and exit
  --documents DOCUMENTS
                        path to the official CORD-19 uncompressed document dataset
  --metadata METADATA   path to the CORD-19 CSV metadata file
```

For example:

```
python3 article_dataset_builder/check_cord19_coverage.py --metadata cord-19/2021-03-22/metadata.csv --documents cord-19/2021-03-22/ --config my_config.json
```

The path for `--documents` is the path where the folder `document_parses` is located. 


## Troubleshooting with imagemagick

Recent update (end of October 2018) of imagemagick is breaking the normal conversion usage. Basically the converter does not convert by default for security reason related to server usage. For non-server mode as involved in our module, it is not a problem to allow PDF conversion. For this, simply edit the file `/etc/ImageMagick-6/policy.xml` (or `/etc/ImageMagick/policy.xml`) and put into comment the following line: 

```
<!-- <policy domain="coder" rights="none" pattern="PDF" /> -->
```

## How to cite 

For citing this software work, please refer to the present GitHub project, together with the [Software Heritage](https://www.softwareheritage.org/) project-level permanent identifier. For example, with BibTeX:

```bibtex
@misc{articledatasetbuilder,
    title = {Article Dataset Builder},
    howpublished = {\url{https://github.com/kermitt2/article_dataset_builder}},
    publisher = {GitHub},
    year = {2020--2023},
    archivePrefix = {swh},
    eprint = {1:dir:adc1581a092560c0ac4a82256c0c905859ec15fc}
}
```

## License and contact

Distributed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

Main author and contact: Patrice Lopez (<patrice.lopez@science-miner.com>)
