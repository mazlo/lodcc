## `graph.tasks`

This package contains executable code for *preparing* a graph-based analysis on the graph topology of RDF datasets. 

---

**Note that**, although it is possible and a reasonable use case to use code without preliminary preparation with the framework, it is still advisable. It facilitates a lot of things, especially when using a database to perform a large-scale analysis on several datasets in parallel.

---

### RDF Dataset Preparation

#### Preparation _without_ a database

To prepare a small bunch of RDF datasets for graph analysis you do not need a database. However, a database enables our framework to look up and store required information, such as the URL and available media types of the RDF dataset that you want to prepare.

---

**Note:** When using the framework without a database, you have to downloaded the RDF dataset beforehand, as the framework does not have the information about where to download it from.

---

To start the preparation use the following command:

```sh
$ python3 -m graph.tasks.prepare --from-file lexvo lexvo_latest.rdf.gz rdfxml
```

**Convention:** The framework will use the `ROOT/dumps/<dataset name>`-folder to read and write corresponding files during execution. 

---

Along with the dataset name you have to pass the name of the main source file of the RDF dump and the correspong format. See `--help` below for details.

After this step the framework will have created a combined and concise *edgelist*, a file that contains per line one labelled edge in the graph structure, of all RDF data related files found in the source file.

---

**Convention:** The framework will name the combined file `data.edgelist.csv`. 

---

#### Preparation _with_ a database

When working with a large set of datasets it is advisable to use a database to persist some required data for graph instantiation and graph analysis. You can use the `--from-db` command-line parameter for that. For example,

```sh
$ python3 -m graph.tasks.prepare --from-db webisalod ecd-linked-data asn-us
```

Database configuration will be read from `ROOT/constants/db.py` file.

---

**Convention:** The framework will use the `ROOT/dumps/<dataset name>`-folder to read and write corresponding files during execution. 

---
 

#### `prepare.py`

`--help` gives you an explanation about the available options.

```sh
$ python3 -m graph.tasks.prepare --help
usage: prepare.py [-h]
                  (--from-file FROM_FILE [FROM_FILE ...] | --from-db FROM_DB [FROM_DB ...])
                  [--overwrite-dl] [--overwrite-nt] [--rm-original]
                  [--keep-edgelists] [--log-debug] [--log-info] [--log-file]
                  [--threads THREADS]

lodcc - A software framework to prepare and perform a large-scale graph-based analysis on the graph topology of RDF datasets.

optional arguments:
  -h, --help            Show this help message and exit
  --from-file FROM_FILE [FROM_FILE ...], -ffl FROM_FILE [FROM_FILE ...]
                        Pass a list of dataset names to prepare. Please pass
                        the filename and media type too. Leave empty to get
                        further details about this parameter.
  --from-db FROM_DB [FROM_DB ...], -fdb FROM_DB [FROM_DB ...]
                        Pass a list of dataset names. Filenames and media
                        types are loaded from database. Specify details in
                        constants/db.py and db.sqlite.properties.
  --overwrite-dl, -ddl  Overwrite RDF dataset dump if already downloaded.
                        Default False.
  --overwrite-nt, -dnt  Overwrite transformed files used to build the graph
                        from. Default False.
  --rm-original, -dro   Remove the initially downloaded RDF dataset dump file.
                        Default False.
  --keep-edgelists, -dke
                        Remove intermediate edgelists, obtained from
                        individual files. A combined data.edgelist.csv file
                        will be generated nevertheless. Default False.
  --log-debug, -ld      Show logging.DEBUG state messages. Default False.
  --log-info, -li       Show logging.INFO state messages. Default True.
  --log-file, -lf       Log into a file named "lodcc.log".
  --threads THREADS, -pt THREADS
                        Number of CPU cores/datasets to use in parallel for
                        preparation. Handy when working with multiple
                        datasets. Default 1. Max 20.
```
