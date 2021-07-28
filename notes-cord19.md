

## CORD-19 version 2020-09-11 

CORD-19 metadata.json file can have multiple rows with the same cord identifier see the FAQ:

https://github.com/allenai/cord19/blob/master/README.md#why-can-the-same-cord_uid-appear-in-multiple-rows

* We observed that CORD-19 metadata.json file can indeed have explicit duplicates. 

However, it is not clear why it is the case, when sources and metadata are identical: 

```
$ grep 'fq4xq00d' metadata.csv 
fq4xq00d,,WHO,Offline: COVID-19—what countries must do now,,,,unk,,2020,"Horton, Richard",Lancet,,#31271,,,,,214779677
fq4xq00d,,WHO,Offline: COVID-19—what countries must do now,,,,unk,,2020,"Horton, Richard",Lancet,,#31270,,,,,214779677
```

We counted 241335 distinct `cord id` out of 253454 entries, a total of 12,119 explicit duplicates.  


* However, for the large majority of duplicate cases, we have the same article, but different `cord id` identifiers. For example:

```
a78dpgjk,,WHO,Evaluating Potential Deceased Donor Renal Transplant Recipients for Asymptomatic COVID-19,,,,unk,,2020,"Ho, Quan Yao; Chung, Shimin Jasmine; Low, Shoen Choon Seng; Chen, Robert Chun; Tee, Swee Ping; Chan, Fu Zi Yvonne; Tan, Ban Hock; Kee, Terence Yi Shern",Transplantation Direct,,#379989,,,,,219879474

6zepsday,d2e68d7eb8b42ae905c460038d091585c354fc5a,Medline; PMC,Evaluating Potential Deceased Donor Renal Transplant Recipients for Asymptomatic COVID-19,10.1097/txd.0000000000001010,PMC7266364,32607425,cc-by-nc-nd,,2020-05-22,"Ho, Quan Yao; Chung, Shimin Jasmine; Low, Shoen Choon Seng; Chen, Robert Chun; Teh, Swee Ping; Chan, Fu Zi Yvonne; Tan, Ban Hock; Kee, Terence Yi Shern",Transplant Direct,,,,document_parses/pdf_json/d2e68d7eb8b42ae905c460038d091585c354fc5a.json,document_parses/pmc_json/PMC7266364.xml.json,https://www.ncbi.nlm.nih.gov/pubmed/32607425/; https://doi.org/10.1097/txd.0000000000001010,219879474
```

```
5gas6xri,,WHO,99mTc-Leukocyte Scintigraphy Revealed Viral Pulmonary Infection in a COVID-19 Patient,,,,unk,"Tc-leukocyte scintigraphy was performed on a 40-year-old woman with spiking fevers. A focus of intense uptake in the right upper thorax was identified, concerning for infection along the central line in the superior vena cava. Additionally, heterogeneously increased uptake in both lungs was noted, which suggested pulmonary infection. CT images of the chest showed patchy ground-glass changes in both lungs and a large consolidation in the right lower lobe, which were consistent with changes for COVID-19 (coronavirus disease 2019). Severe acute respiratory syndrome coronavirus 2 RNA test was positive. This case demonstrates that leukocyte uptake in bilateral lungs could reveal viral pulmonary infection in COVID-19.",2020,"Zheng, Jiefu; Liu, Yiyan",Clin. nucl. med,,#672034,,,,,221562769

65enq06o,23988b36eee15a54f905ec54811ca232192f10d6,Medline; PMC,(99m)Tc-Leukocyte Scintigraphy Revealed Viral Pulmonary Infection in a COVID-19 Patient,10.1097/rlu.0000000000003219,PMC7473795,32701817,no-cc,"(99m)Tc-leukocyte scintigraphy was performed on a 40-year-old woman with spiking fevers. A focus of intense uptake in the right upper thorax was identified, concerning for infection along the central line in the superior vena cava. Additionally, heterogeneously increased uptake in both lungs was noted, which suggested pulmonary infection. CT images of the chest showed patchy ground-glass changes in both lungs and a large consolidation in the right lower lobe, which were consistent with changes for COVID-19 (coronavirus disease 2019). Severe acute respiratory syndrome coronavirus 2 RNA test was positive. This case demonstrates that leukocyte uptake in bilateral lungs could reveal viral pulmonary infection in COVID-19.",2020-07-22,"Zheng, Jiefu; Liu, Yiyan",Clin Nucl Med,,,,document_parses/pdf_json/23988b36eee15a54f905ec54811ca232192f10d6.json,,https://www.ncbi.nlm.nih.gov/pubmed/32701817/; https://doi.org/10.1097/rlu.0000000000003219,221562769
```

We counted 161839 distinct entries out of 253454 entries, a total of 91,257 duplicates: 12,119 explicit duplicates and 79,138 article-level duplicates.  

```
total entries from CORD-19 metadata file (including all duplicates): 253454
total distinct entries: 161839
total duplicates: 91257
total duplicated crod id: 12119 (explicit duplicates)
total article-level duplicates: 79138 
```

* Result from our harvesting (given by option `--disgnostic`):

```
* 
total entries: 161839
---
total valid entries: 138440 entries with valid OA URL and PDF and TEI XML
---
total invalid OA URL: 20697
total entries with valid OA URL: 141142
---
total invalid PDF: 22274
total entries with successfully downloaded PDF: 139565
---
total invalid TEI: 23399
total entries with successfully convereted TEI XML: 138440
---
total entries with GROBID TEI file: 138432
total entries with Pub2TEI TEI file: 104288
total entries with at least one TEI file: 140322

139192 PDF -> 138432 TEI XML (760 GROBID conversion failures)
104288 nxlm -> 104288 TEI XML (but 45 tranformations reported as containing some kind of failure)

entries with at least one fulltext tei file -> 140322
```

CORD-19 dataset covergae check (see `check_cord19_coeverage.py`):

```
253454 entries
241335 distinct cord id

total distinct PMC-derived JSON: 77115
total distinct PDF-derived JSON: 94541
total distinct entry with at least one JSON: 103587
```

Dataset "official" reported numbers see https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-09-11/changelog:

```
PDF - 106144 json 
PMC - 77321 json
```


## CORD-19 version 5 

Here are the results regarding the CORD-19 version 5 ([metadata.csv](https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/2020-03-28/metadata.csv)) to illustrate the interest of the tool:

|   | official CORD-19 | this harvester |
|---|---|---|
| total entries | 45,828 | 45,828 | 
| entries with valid OA URL | - | 42,742|
| entries with successfully downloaded PDF | - | 42,362 | 
| entries with structured full texts via GROBID | ~33,000 (JSON) | 41,070 (TEI XML) |
| entries with structured full texts via PMC JATS | - | 15,955 (TEI XML) |
| __total entries with at least one structured full text__ | __~33,000 (JSON)__ | __41,609 (TEI XML)__ |


