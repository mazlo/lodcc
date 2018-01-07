[![pipeline status](https://git.gesis.org/matthaeus/lodcc/badges/master/pipeline.svg)](https://git.gesis.org/matthaeus/lodcc/commits/master)

#### Commands

##### dbpedia 

- Download all filenames of all datasets into a file `dbpedia-link.txt`

```sh
curl -L http://downloads.dbpedia.org/2016-10/core-i18n/en/ -o dbpedia-link.txt
```

- Filter unnecessary datasets and prefix with url

```sh
cat dbpedia-link.txt | cut -d '"' -f2 | egrep -i "ttl" | egrep -i -v "wkd|sorted|nested" | sed 's#^\(.*\)#http://downloads.dbpedia.org/2016-10/core-i18n/en/\1#' | sed -n '2,60p' > dbpedia-links.txt
```