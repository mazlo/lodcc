[![DOI](https://zenodo.org/badge/128509275.svg)](https://zenodo.org/badge/latestdoi/128509275)

# Analysis of Graph Measures on RDF Graphs

The main purpose of the framework is to prepare and perform a graph-based analysis on the graph topology of an RDF dataset. The main challenges were to do that on large scale and with focus on performance, i.e. with large state-of-the-art RDF graphs (hundreds of millions of edges) and in parallel with many datasets at once. 

The framework is capable of dealing with the following:
 
* Packed data dumps. Various formats are supported, like bz2, 7zip, tar.gz, etc. This is achieved by utilizing the unix-tool [dtrx](https://brettcsmith.org/2007/dtrx/).
* Archives, which contain a hierarchy of files and folders, will get scanned for files containing RDF data. Other files will be ignored, e.g. Excel- or text-files, etc.
* Any files containing other formats than N-Triples are transformed (if necessary). The list of supported formats is currently limited to the most common ones for RDF data, which are N-Triples, RDF/XML, Turtle, N-Quads, and Notation3. This is achieved by utilizing [rapper](http://librdf.org/raptor/).

## TLDR;

### Example commands

##### Prepare several RDF datasets for graph-analysis in parallel

`python lodcc/lodcc.py --prepare-graph --from-db --use-datasets core education-data-gov-uk webisalod --threads 3`

This command will download (if not present), transform (if necessary), and prepare an edgelist ready to be read as graph-structure. `--from-db` loads the appropriate formats and urls from database configured in `db.properties`.

##### Run analysis on several prepared RDF datasets in parallel

`python lodcc/lodcc.py --build-graph --from-file core education-data-gov-uk webisalod --threads 2 --threads-openmp 8 --features diameter --print-stats`

This command loads the edgelists of the three given datasets `--from-file`, 2 in parallel. For each of them a graph-structure is created and the `diameter` feature is computed. Results will be printed to standard out.

## Documentation

### Installation requirements

The framework is build with python2 and relies on many other libraries. It has been developed and tested in a linux-environment (debian jessie) with shell. Please make sure you have installed all required libraries.

##### Unix tools

- `curl`, the downloading RDF dataset dumps.

- `dtrx`, for the extraction of data dumps. You can [download it here](https://brettcsmith.org/2007/dtrx/), but there are also [packages provided](https://reposcope.com/package/dtrx) by various linux distributions.

- `rapper`, a tool by the [Raptor RDF Syntax Library](http://librdf.org/raptor/), for the transformation of various rdf formats into n-triples. There are [packages provided](https://reposcope.com/package/raptor2-utils) by various linux distributions.

##### Python libraries

- `graph_tool`, a python library for statistical analysis of graphs. Installation instructions can be found [here](https://graph-tool.skewed.de/static/doc/index.html).

- `requirements.txt`. Please find requirements of further python modules in this file. Please install them with `pip install -r requirements.txt`

#### Configuration

1. If you are using a database (which is optional, but convenient) please configure it in `db.properties` file. You can find a `db.properties.example` file in the root folder of the project. 

2. Please create a `dumps` folder located in the root folder of the project structure. The program will look into this folder in all the following commands.

### Usage

At the top-level, the framework supports three main functions, which are:

#### Step 1: Acquire Metadata (optional)

##### Command example

`python lodcc/lodcc.py --parse-datapackages`

##### Details on `--parse-datapackages`

This command requires a database to be configured beforehand. The table has to provide at least a list of data sets (e.g. from the current LOD Cloud) with at least: `| id | name | url |` of the data package. An initial table setup can be found in [db/01-create-table-mysql.sql](db/02-init-table-stats-mysql.sql)

The metadata is first crawled from the web (datahub.io). Then relevant information are parsed (from `datapackage.json` file), like the format and the urls to obtain the data dump from, and stored in the table.

The program will respect the [formats.properties](formats.properties) file, in order to map non-official format statements to the official mime-type.

##### Results from this step

A database table will be extended by the list of available formats for all data sets. Each format will be written to its own column. 

[db/03-init-table-stats-result-step1-mysql.sql](db/03-init-table-stats-result-step1-mysql.sql) shows an initialized table after execution (last modified: 2017-12-01).

##### Mandatory parameters
   
None.

##### Optional parameters

None.

#### Step 2. Prepare Datasets for Graph-based Analysis (optional)

##### Command example

`python lodcc/lodcc.py --prepare-graph --use-datasets education-data-gov-uk`

##### Details on `--prepare-graph`

This step is optional, but very convenient if you haven't prepared an edgelist from an RDF dataset before. You will need to decide if to use the database or a local file to read some required information (basically the filename and format). In case using a database you will need to provide the official mime-types in the columns for the datasets, e.g. `application_n_triples`. An example table setup can be found in [db/03-init-table-stats-result-step1-mysql.sql](db/03-init-table-stats-result-step1-mysql.sql).

The prepare-graph command creates a compact edgelist representation for each RDF dataset that is passed to the command. This is achieved by 

1. downloading each RDF dataset dump first, if not present.
2. Extracting the dump, if necessary.
3. Transforming the file (all files, if there is a nested folder structure, ignoring (many) non-RDF formats) into n-triples, if necessary.
4. Making a hashed edgelist from (all) ntriple-files and combining this into a file called `data.edgelist.csv`.

##### Mandatory parameters
   
- `--from-db`, uses the database to read urls and formats from.

   * Pass a list of dataset names with `--use-datasets [DATASET [DATASET ...]]`
   
- `--from-file DATASET FILENAME FORMAT [--from-file ... [...]]`, if a local file should be used. More than one file may be given. You will need to provide the name and format for each, e.g. `--from-file webisalod data-dump.n3 n3 --from-file oecd-linked-data file.nt ntriples`. 

##### Results from this step

A `data.edgelist.csv` file for each of the given datasets in the corresponding folder of the dataset (i.e. `PROJECT_ROOT/dumps/DATASET/data.edgelist.csv`). This file represents the edgelist of the RDF dataset dump and can be loaded efficiently by the graph_tool library.

##### Optional parameters
   
- `--overwrite-dl`. If this argument is present, the program WILL NOT use data dumps which were already dowloaded, but download them again. Default: FALSE.

- `--overwrite-nt`. If this argument is present, the program WILL NOT use ntriple files which were already transformed, but transform them again. Default: FALSE.

- `--rm-original`. If this argument is present, the program WILL REMOVE
the original downloaded data dump file when the edgelist if created. Default: FALSE.

- `--keep-edgelists`. If the downloaded data dump consists of several files (compressed archive) and this argument is present, the program WILL KEEP single edgelists which were generated. A data.edgelist.csv file will be generated nevertheless. Default: FALSE.

- `--threads THREADS`. Control parallel execution. There will be only `THREADS` datasets handled at a time. Default: 1.

#### Step 3: Execute Graph-based Analysis

##### Command example

`python lodcc/lodcc.py --build-graph --from-file asn-us`

##### Details on `--build-graph`

With this command a graph-structure, for each RDF dataset that is passed to the command, will be created from the corresponding edgelist. If no further parameters are provided the graph analysis will be done for all measures.

The program will first look for a `data.graph.gt.gz` file to load the graph-structure from, else `data.edgelist.csv` is considered. Otherwise an error is thrown.

##### Mandatory parameters

- `--from-db`, uses the database to read and store values for measures.

   * Pass a list of dataset names with `--use-datasets [DATASET [DATASET ...]]`
   
- `--from-file DATASET [--from-file DATASET [...]]`, if a local file should be used. More than one file may be given. You will need to provide the name of the dataset, e.g. `--from-file webisalod --from-file oecd-linked-data`. 

##### Results from this step

Results on the graph-based analysis on the RDF dataset, either stored in database or printed to standard out. In case of using a database, please ensure to have an initial table structure. An example can be found in [db/01-create-table-mysql.sql](01-create-table-mysql.sql).

##### Optional parameters

- `--dump-graph`. If this argument is present, the program will dump the graph, loaded from the edgelist in `data.edgelist.csv`, to a binary file  `data.graph.gt.gz`, that is much more convenient to handle in future analyzses.

- `--reconstruct-graph`. If this argument is present, the program will ignore an existing `data.graph.gt.gz` file and will reload the graph from the edgelist, given in `data.edgelist.csv`. Default: FALSE.

- `--features [FEATURE [FEATURE ...]]`. If present, the graph analysis will be done only for the given list of features, e.g. `--features h-index fill diameter`. Default: `degree, plots, diameter, centralization, fill, h_index, pagerank, parallel_edges, powerlaw, reciprocity`. Other possible values: `local_clustering, global_clustering, eigenvector_centrality`. Please note that these measures are computationally intensive.

- `--skip-features [FEATURE [FEATURE ...]]`. If present, the graph analysis will not be done on the given list of features, e.g. `-skip-features parallel_edges`.

- `--threads-openmp`. If present, this value will be passed to the OS, in order to configure parallel computation of features provided by the graph_tool library. Default: 7.

- `--print-stats`. If present, the program will print results on the analysis to standard out. If database is configured, the program will not save values in the database.

#### Global parameters

- `--log-debug`. If present, the program will log in debug mode. Default: FALSE.

- `--log-info`. If present, the program will log in info mode. Default: TRUE.

## License

This package is licensed under the MIT License.

## How to Cite

Please refer to the DOI for citation. You can cite all versions of this project by using the DOI [10.5281/zenodo.2109469](https://doi.org/10.5281/zenodo.2109469). This DOI represents all versions, and will always resolve to the latest one.

