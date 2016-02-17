# Solr output plugin for Embulk

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **host**: solr host name. (string, required)
- **port**: port number of solr. (int, default: `"8080"`)
- **collection**: collection name which you want documents put into. (string, required)
- **bulkSize**: maximum number of documents sending solr at onece. (int, default: `"1000"`)

### Modes

this plugin support only one mode and you don't need to set mode explicitly.
the default mode update/insert a document. so if a document has already existed in the collection, it will be updated. if not, it will be inserted into the collection as new document.

## Example

```yaml
out:
  type: solr
  host: localhost
  port: 8080
  collection: mytest
  bulkSize: 500
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
