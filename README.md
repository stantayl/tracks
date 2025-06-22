# tracks
Small Scala Spark testbed for playing with various DataFrame constructs.

Data source is based on a tag scrape of my MP3 collection, and the Spark code
provides stats on the collection as well as flagging files whose tags are in need
of cleanup/editing.

- `scripts/scrape_tracks.py` is the script used to walk the file collection
  and output the relevant ID3 tags
- `src/main/resources/tracks.tsv` contains the last scrape performed
  (TODO: not actually used as a "resource"; still loading this from an absolute path)

## Command line usage

```
tracks -s <datasource> [-i input-path] [-o output-path] [commands]
```
where:

`datasource` is one of:
- **csv** loads denormalized flat data from the `tracks.tsv` file


- **postgres** in a separate DBT project I loaded `tracks.tsv` into a local Postgres
  instance so I could cross-check my Spark against straight SQL queries (TODO: I should
  really rename the **csv** option to **tsv**)


- **json** reads the nested JSON data written by `generate-json` (see below), for a
  testbed of nested data; just some simple stuff so far - none of the commands below 
  presently work with the JSON data.

`commands` are one or more of:
- **stats** generates basic track/album/artist counts from the denormalized flat source data
 
 
- **show** shows the results of denormalization into the normalized `tracks` DataFrame, as
  well as surrogate key `artists`, `albums`, and `genres` DataFrames
  
  
- **extended-stats** more interesting outputs from the normalized tables
  
 
- **validations** checking for invalid and missing column values
  
  
- **windowed-aggs** some basic outputs demonstrating use of windowed SQL
  
 
- **generate-json** generates nested JSON from the normalized DataFrames
  - `src/main/resources/tracks.json` is a saved output from running this
  - `src/main/resources/sample.json` is a pretty-printed sample for reference 

If `commands` is not supplied, then everything except `generate-json` is run by default

Test