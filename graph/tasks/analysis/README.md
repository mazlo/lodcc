## `graph.tasks.analysis`

This package contains executable code for performing a graph-based analysis on RDF datasets. 

---

**Note:** You need to have an *edgelist* or binary materialization of a *graph-object* at hand, to be able to do the analysis. The framework uses both to create the graph-object in memory and to compute the measures on that object. 

---

**Note:** Although it is possible and a reasonable use case to use code without preliminary preparation with the framework, it is still advisable, as it facilitates a lot of things. See [this README](../README.md) on how to prepare an RDF dataset for graph analysis.

---

#### Analysis of single RDF graphs

To start an analysis on, say 3 datasets, you do not need a database. You can use the `--from-file` command-line parameter like in the following command: 

```sh
$ python3 -m graph.tasks.analysis.core_measures --from-file webisalod ecd-linked-data asn-us --features fill reciprocity
```

**Convention:** The framework will use the `ROOT/dumps/<dataset name>`-folder to read and write corresponding files during execution. 

---

For graph instantiation, e.g. of the dataset with name `ecd-linked-data`, it expects either of the two files to be located in `ROOT/dumps/ecd-linked-data/`: 

1. `data.edgelist.csv`, a source-vertex target-vertex mapping file, representing a graph edge per line, or
2. `data.graph.gt.gz`, a binary materialized graph-object previously obtained from graph-tool.

After graph instantiation the framework will compute the requested measures. It will print the values to STDOUT, if you add the `--print-stats` parameter.

#### Analysis of a large sets of RDF graphs

For a large set of datasets it is advisable to use a database to persist the requested measure values. You can use the `--from-db` command-line parameter for that. For example,

```sh
$ python3 -m graph.tasks.analysis.core_measures --from-db webisalod ecd-linked-data asn-us --features fill reciprocity
```

Database configuration will be read from `ROOT/constants/db.py` file.

For graph instantiation, the framework gets the path to the corresponding files from either of the two columns `path_edgelist` or `path_graph_gt`.

However, it will use the `ROOT/dumps/<dataset name>/` folder for writing plots etc.
 
### Measures

There are two sets of measures to choose from.

#### `core_measures.py`

`--help` gives you an explanation about the available options.

```sh
$ python3 -m graph.tasks.analysis.core_measures --help
usage: core_measures.py [-h]
                        (--from-file FROM_FILE [FROM_FILE ...] | --from-db FROM_DB [FROM_DB ...])
                        [--print-stats] [--threads THREADS]
                        [--sample-vertices] [--sample-size SAMPLE_SIZE]
                        [--openmp-disabled] [--threads-openmp THREADS_OPENMP]
                        [--do-heavy-analysis]
                        [--features [FEATURES [FEATURES ...]]]
                        [--skip-features [SKIP_FEATURES [SKIP_FEATURES ...]]]

lodcc - A software framework to prepare and perform a large-scale graph-based analysis on the graph topology of RDF datasets.

optional arguments:
  -h, --help            Show this help message and exit
  --from-file FROM_FILE [FROM_FILE ...], -ffl FROM_FILE [FROM_FILE ...]
                        Pass a list of dataset names. Indicates that measure
                        values will be written to a file called
                        "measures.<dataset name>.csv".
  --from-db FROM_DB [FROM_DB ...], -fdb FROM_DB [FROM_DB ...]
                        Pass a list of dataset names. Indicates that further
                        details and measure values are written to database.
                        Specify details in constants/db.py and
                        db.sqlite.properties.
  --print-stats, -lp    Prints measure values to STDOUT instead of writing to
                        db or file. Default False.
  --threads THREADS, -pt THREADS
                        Number of CPU cores/datasets to use in parallel for
                        graph analysis. Handy when working with multiple
                        datasets. Default 1. Max 20.
  --sample-vertices, -gsv
                        not yet supported
  --sample-size SAMPLE_SIZE, -gss SAMPLE_SIZE
                        not yet supported
  --openmp-disabled, -gto
                        Pass if you did not have OpenMP enabled during
                        compilation of graph-tool. Default False.
  --threads-openmp THREADS_OPENMP, -gth THREADS_OPENMP
                        Number of CPU cores used by the core graph-tool
                        library. See also --openmp-disabled. Default 8.
  --do-heavy-analysis, -gfsh
                        Obsolete. See --skip-features.
  --features [FEATURES [FEATURES ...]], -gfs [FEATURES [FEATURES ...]]
                        Give a list of graph measures to compute, e.g., "-gfs
                        degree diameter" for all degree-related measures and
                        the diameter. Default is the full list of less
                        computation intensive graph measures. See also
                        constants/measures.py.
  --skip-features [SKIP_FEATURES [SKIP_FEATURES ...]], -gsfs [SKIP_FEATURES [SKIP_FEATURES ...]]
                        When --features not given, specify the list of graph
                        measures to skip. Default [].
```

### `rdf_measures.py`

`--help` gives you an explanation about the available options.

tbc
