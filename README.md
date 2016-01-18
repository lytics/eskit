# Elasticsearch Copier

Toolkit for copying and validating Elasticsearch indexes.

* `escp` copies an index
* `esdiff` compares documents in two indexes; intended for validating copies

## Usage
```sh
# Install all utilities with go get:
go get -v github.com/lytics/escp/...
```

```sh
# Copy srcindex on host1 to dstindex on host2
escp http://host1:9200/srcindex/_search http://host2:9200/_bulk dstindex
```

```sh
# Check document counts are equal and spot check documents
esdiff http://host1:9200/srcindex/_search http://host2:9200/dstindex

# Check 25% of documents
esdiff -d 4 http://host1:9200/srcindex/_search http://host2:9200/dstindex

# Check all documents
esdiff -d 1 http://host1:9200/srcindex/_search http://host2:9200/dstindex
```

Other Tools
-------------------------------
* https://github.com/taskrabbit/elasticsearch-dump
* https://github.com/mallocator/Elasticsearch-Exporter
* http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-snapshots.html
