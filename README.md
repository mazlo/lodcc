[![pipeline status](https://git.gesis.org/matthaeus/lodcc/badges/master/pipeline.svg)](https://git.gesis.org/matthaeus/lodcc/commits/master)

### Install dependencies

```sh
$ cd lodcc/
$ pip install -r requirements.txt
$ sudo apt-get install dtrx raptor2-utils
```

### Commands

#### Tests

- Small dataset

`python lodcc.py --parse-resource-urls --use-datasets museums-in-italy --log-level-debug`

- Larger dataset

`python lodcc.py --parse-resource-urls --use-datasets pokepedia-fr --log-level-debug`

- Multithreaded processing

`python lodcc.py --parse-resource-urls --use-datasets museums-in-italy pokepedia-fr --threads 2`

#### File system

- Determine file sizes of all dumps

```sh
$ find dumps/ -type f -exec ls -s --block-size=M {} \; > dumps-sizes.txt
$ cat dumps-sizes.txt | sed -e '/edgelist/! s/^.*$/###/' -e '/^###/D' | sort -h -r | less
```

#### Database

- Start postgresql with docker
 
```sh
$ docker run --name cloudstats-postgres-9.4 -p 5432:5432 -e PGDATA=/home/cloudstats/var/lib/postgresql/data -e POSTGRES_USER=cloudstats -e POSTGRES_PASSWORD=cloudstats -d postgres:9.4
```

- import data from database dump

```sh
$ cat pgdumpall.sql | docker exec -i cloudstats-postgres-9.4 psql -U cloudstats
```

#### dbpedia 

- Download all filenames of all datasets into a file `dbpedia-link.txt`

```sh
$ curl -L http://downloads.dbpedia.org/2016-10/core-i18n/en/ -o dbpedia-link.txt
```

- Filter unnecessary datasets and prefix with url

```sh
$ cat dbpedia-link.txt | cut -d '"' -f2 | egrep -i "ttl" | egrep -i -v "wkd|sorted|nested" | sed 's#^\(.*\)#http://downloads.dbpedia.org/2016-10/core-i18n/en/\1#' | sed -n '2,60p' > dbpedia-links.txt
```