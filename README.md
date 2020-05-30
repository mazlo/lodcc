[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.2109469.svg)](https://doi.org/10.5281/zenodo.2109469)

# A Software Framework for the graph-based Analysis on RDF Graphs

This framework enables to prepare and perform a graph-based analysis on the graph topology of RDF datasets. One of the main goals were to do that on large-scale and with focus on performance, i.e., with large state-of-the-art RDF graphs (hundreds of millions of edges) and in parallel, with many datasets at once. 

[A recent analysis](https://arxiv.org/abs/1907.01885) on 280 datasets from the [LOD Cloud](https://lod-cloud.net/) 2017 has been conducted with this framework. Please find here [the results](https://github.com/mazlo/lod-graph-analysis) on 28 graph measures as [a browsable version](http://data.gesis.org/lodcc/2017-08) of the study. Also, the results are available as [a citable resource](https://doi.org/10.5281/zenodo.1214433) at [Zenodo](https://zenodo.org/). 

| __Domain__ | __Datasets analyzed__ | __Max. # of Vertices__ | __Max. # of Edges__ | __Avg. # of Vertices__ | __Avg. # of Edges__ | 
| ---------- | ------------------: | ----------------------: | -------------------: | ----------------------: | -------------------: | 
| Cross Domain | 15 | 614,448,283 | 2,656,226,986 | 57,827,358 | 218,930,066 |
| Geography | 11 | 47,541,174 | 340,880,391 | 9,763,721 | 61,049,429 |
| Government | 37 | 131,634,287 | 1,489,689,235 | 7,491,531 | 71,263,878 |
| Life Sciences | 32 | 356,837,444 | 722,889,087 | 25,550,646 | 85,262,882 |
| Linguistics | 122 | 120,683,397 | 291,314,466 | 1,260,455 | 3,347,268 |
| Media | 6 | 48,318,259 | 161,749,815 | 9,504,622 | 31,100,859 |
| Publications | 50 | 218,757,266 | 720,668,819 | 9,036,204 | 28,017,502 |
| Social Networking | 3 | 331,647 | 1,600,499 | 237,003 | 1,062,986 |
| User Generated | 4 | 2,961,628 | 4,932,352 | 967,798 | 1,992,069 |

### Goodies

RDF data dumps are preferred (so far). The framework is capable of dealing with the following:

* Automatic downloading of the RDF data dumps before preparation.
* Packed data dumps. Various formats are supported, like bz2, 7zip, tar.gz, etc. This is achieved by employing the unix-tool [dtrx](https://brettcsmith.org/2007/dtrx/).
* Archives, which contain a hierarchy of files and folders, will get scanned for files containing RDF data. Files which are not associated with RDF data will be ignored, e.g. Excel-, HTML-, or text-files.
* The list of supported [RDF media types](https://www.w3.org/2008/01/rdf-media-types) is currently limited to the most common ones for RDF data, which are N-Triples, RDF/XML, Turtle, N-Quads, and Notation3. Any files containing these formats are transformed into N-Triples while graph creation. The transformation is achieved by employing the cli-tool [rapper](http://librdf.org/raptor/). 

Further:

+ The framework is implemented in Python. The list of supported graph measures is extendable.
+ There is a ready-to-go docker-image available, with all third-party libraries pre-installed.

Currently ongoing and work in progress:

+ Query instantiation from graph representation, and
+ Edge- and vertex-based graph sampling.

## Documentation

### Installation

Installation instructions can be found in [`INSTALL`](INSTALL).

### Project Structure

In each of the subpackages you will find a detailed README file. The following table gives you an overview of the most important subpackages.

| Package | Description |
| :------ | :---------- |
| `constants` | Contains files which hold some static values. Some of them are configurable, e.g., `datapackage.py` and `db.py` | 
| `datapackages` | Contains code for (optional) pre-processing of datahub.io related datapackage.json files. |
| `db`      | Contains code to connect to a (optional) local database. A local database stores detailed information about dataset names, URLs, available RDF media types, etc. This is parsed by the `datapackage.parser`-module. | 
| `graph`   | This is the main package which contains code for RDF data transformation, edgelist creation for graph building, graph measure computation, etc. | 
| `query`   | Contains code for query generation from query templates. |
| `util`    | Utility subpackage with helper modules, used by various other modules. |


### Usage

Executable code can be found in each of the corresponding `*.tasks.*` subpackages, i.e., 

| Tasks Package | Task Description |
| ------- | ----------- |
| [`datapackage/tasks/*`](datapackage/tasks/README.md) | for an optional preliminary step to acquire metadata for datasets from [datahub.io](http://old.datahub.io). |
| [`graph/tasks/*`](graph/tasks/README.md) | for a preliminary preparation process which turns your RDF dataset into an edgelist. |
| [`graph/tasks/analysis/*`](graph/tasks/analysis/README.md) | for graph-based measure computation of your graph instances. |

Please find more detailed instructions in the README files of the corresponding packages. 

#### Example commands

The software is suppossed to be run from command-line on a unix-based system.

##### 1. Prepare RDF datasets for graph-analysis

```
$ python3 -m graph.tasks.prepare --from-db core education-data-gov-uk webisalod --threads 3
```

This command will (1) download (if not present), (2) transform (if necessary), and (3) prepare an RDF dataset as an edgelist, ready to be instantiated as graph-object. 

- `--from-db` used to load dataset URLs and available formats from an sqlite-database configured in `db.sqlite.properties`.
- `--threads` indicates the number of datasets that are handled in parallel.

##### 2. Run an analysis on the prepared RDF datasets in parallel

```
$ python3 -m graph.tasks.analysis.core_measures --from-file core education-data-gov-uk webisalod --threads 2 --threads-openmp 8 --features diameter --print-stats
```

This command instantiates the graph-objects, by loading the edgelists or the binary graph-objects, if available. After that, the graph measure `diameter` will be computed in the graphs. 

- `--from-file` used here, so measure values will be printed to STDOUT. 
- `--threads` indicates the number of datasets that are handled in parallel.

## License

This package is licensed under the MIT License.

## How to Cite

Please refer to the DOI for citation. You can cite all versions of this project by using the canonical DOI [10.5281/zenodo.2109469](https://doi.org/10.5281/zenodo.2109469). This DOI represents all versions, and will always resolve to the latest one.

