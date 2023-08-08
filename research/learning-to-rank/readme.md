# Learning To Rank

Learning To Rank plugin utilizes a trained model to rank the search results. The plugin is based on the [Ranklib](https://sourceforge.net/p/lemur/wiki/RankLib/) library.

## Installation

The plugin can be installed using the `plugin` command (example is in the [Dockerfile](docker/Dockerfile)):

```bash
$ bin/elasticsearch-plugin install ...
```

## Configuration

The plugin utilizes a special metadata index, in which it stores both feature sets and models.

`configure-ltr.py` creates the index, feature set and inserts a model into the index. Example usage:

```bash
$ python configure-ltr.py \
    --metadata_index ltr-metadata-index \
    --feature_set ltr-feature-set \
    --model_name exp6-8 \
    --model_path exp6-8.model \
    --reset
```

The connection details should be specified in the environment variables:

```bash
$ ES_HOST=localhost ES_PORT=9200 ES_USERNAME=elastic ES_PASSWORD=changeme python configure-ltr.py ...
```

### Glossary

* feature store - a special index in which the feature sets are stored
* feature set - a set of features that are used to rank the results
* model - a trained model that is used to rank the results

## Dependencies

There is a slight problem with dependencies. It must use Python's elasticsearch client of version 7.x.x, since the plugin's client has not been updated to the 8.x.x versions. Thus, the dependencies for this script are different from the **Name Generator** and you should be aware of that.

## Other scripts

* `search.py` - copied almost entirely from [ML Ranking PR](https://github.com/namehash/name-generator/pull/206)
* `populate.py` - copied almost entirely from [name-generator/research/elasticsearch](https://github.com/namehash/name-generator/tree/master/research/elasticsearch)
