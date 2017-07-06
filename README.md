
![alt text](docs/images/escroll.png "escroll")
---
[![Build Status](https://travis-ci.org/berglh/escroll.svg?branch=master)](https://travis-ci.org/berglh/escroll)
**escroll** is a command line client written in *Go* that takes the hassle out of scroll searches in elasticsearch. It aims to be;

- *Simple*: make *elasticsearch* scroll searches easy
- *Safe*: prevents use of `delete_by_query` and `XDELETE`
- *Familiar*: just like a regular search, emulate *curl* usage
- *Quick*: lightweight tool, minimal overhead for large dataset extraction
- *Predictable*: useful progress output

**escroll** was made to eliminate the time consuming task of writing bespoke scroll search scripts to return *every* result from a regular elasticsearch query. It is a safe and re-usable tool for big data extraction from *elasticsearch*.


- [Usage](#usage)
  - [Arguments](#arguments)
  - [Switches](#switches)
- [Output](#output)
- [Examples](#examples)
---

## Usage

#### Arguments
Flag | Example | Description
:---:|:----|:---
`-url` | `http://localhost:9200/index/_search?scroll=10s` | The elasticsearch node, port number and query segments of the URI. Scroll time-out is mandatory.
`-d` | *`query string`* or `@query.json` | The query request body DSL, prefix string with `@` for DSL query file.

#### Switches
Flag | Description
:---:|:----
`-p`| Enables pretty JSON formatting instead of JSON lines.

**Note:** By default, the size of each scroll is 10, the default search result size in *elasticsearch*. To increase the speed of the scroll search, scale this up to more efficient size in the query request body. The default maximum search result size in *elasticsearch* is `10000`.


## Output

Information about the progress of the scroll search is sent to `stderr`.

**escroll** outputs the data only inside the `hits.hits._source[]` results array as **JSON** *lines* to `stdout`. 

The primary purpose of this tool is to extract event payload data, not for *elasticsearch* index backup. This means that `hits._index`, `hits._type`, `hits._id`, `hits._score` fields along with the search statistic are not included in the scroll output to `stdout`.

All data on `stdout` is actual event data from *elasticsearch* to be redirected to a file or piped to another process:

```
{"user" : "berg", "@timestamp" : "2017-04-13T14:41:12", "message": "trying out escroll"}
{"user" : "berg", "@timestamp" : "2017-04-13T14:42:34", "message": "looking good"}
{"user" : "berg", "@timestamp" : "2017-04-13T14:42:58", "message": "these JSON lines"}
```

The event output can also be *pretty* **JSON** formatted, similar to ruby debug format in *logstash* but in valid **JSON**:
```
{
    "user" {
        "uid" : "berglh",
        "realm" : "github.com"
    },
    "@timestamp" : "2017-04-13T14:46:23",
    "message": "pretty JSON event",
    "country": "Taiwan",
    "city": "Taipei",
    "ranking": 1
}
```


## Examples

A basic query with the query request body as a string:

```
~$ escroll -url 'http://esnode:9200/twitter/tweet/_search?scroll=30s' -d '{"query":{"term":{"user":"berg"}}}'
```

A basic query with the query request body as an input file with size specified:

```
~$ escroll -url 'http://esnode:9200/index/_search?scroll=1m' -d @query.json
```
query.json contents:
```
{
    "size": "1000",
    "query": {
         "term": {
            "user": "berg"
        }
    }
}
```

To limit the fields returned on a query, you can use same parameters in a URI search. When using the URI search parameters to return specific **JSON** paths, the `hits.total`, `_scroll_id` and `hits.hits._source` paths are required for a successful **escroll**. The URI query segment would look like this:

```
'../_search?scroll=30s&filter_path=hits.total,hits.hits._source,_scroll_id&_source=@timestamp,user.uid'
```
