# Solr output plugin for Embulk

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **host**: solr host name. (string, required)
- **port**: port number of solr. (int, default: `"8983"`)
- **collection**: collection name which you want documents put into. (string, required)
- **bulkSize**: maximum number of documents sending solr at onece. (int, default: `"1000"`)
- **idColumnName**: id column name. (string, required)
- **multiValuedField**: multiValued field column name. (string, optional)

### Modes

this plugin support only one mode and you don't need to set mode explicitly.
the default mode update/insert a document. so if a document has already existed in the collection, it will be updated. if not, it will be inserted into the collection as new document.

## About multi-valued

Embluk does not allow multi-column with same name in a record. so if you want to feed multiValued field, you need to split data first. see this config example.

A column 'category' have multi values delimited with '||'. (ex. 'Animation||Drama||War')

```yaml
filters:
- type: split
  delimiter: '||'
  keep_input: true
  target_key: category
out:
  type: solr
  host: localhost
  port: 8983
  collection: mytest
  bulkSize: 1000
  idColumnName: id
  multiValuedField:
    - category
```

After split data, record previews as following.
```
| id:string | title:string     | category:string |
|         1 | Toy Story (1995) |       Animation |
|         1 | Toy Story (1995) |      Children's |
|         1 | Toy Story (1995) |          Comedy |
```


Also note this plugin assume records are sorted by id column value. So even if you don't need to use split plugin, you need to sort record to feed multiValued field.

## Example

```yaml
out:
  type: solr
  host: localhost
  port: 8983
  collection: mytest
  bulkSize: 500
  idColumnName: id
  multiValuedField:
    - category
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```


## How to send Pull Request 

If you would like to send a patch or Pull Request to this repository, please agree with our CLA before that. Please check following steps.

1. You send Pull Request to our Yahoo! JAPAN OSS.
2. We send you CLA to get agreement from you.
  - Yahoo! JAPAN CLA　https://gist.github.com/ydnjp/3095832f100d5c3d2592
3. You agree with the CLA.
4. We review your Pull Request and merge it. 
