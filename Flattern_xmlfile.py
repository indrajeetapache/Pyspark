import xml.etree.ElementTree as et
from collections import defaultdict
import pandas as pd


def flatten_xml(node, key_prefix=()):
    """
    Walk an XML node, generating tuples of key parts and values.
    """

    # Copy tag content if any
    text = (node.text or '').strip()
    if text:
        yield key_prefix, text

    # Copy attributes
    for attr, value in node.items():
        yield key_prefix + (attr,), value

    # Recurse into children
    for child in node:
        yield from flatten_xml(child, key_prefix + (child.tag,))


def dictify_key_pairs(pairs, key_sep='-'):
    """
    Dictify key pairs from flatten_xml, taking care of duplicate keys.
    """
    out = {}

    # Group by candidate key.
    key_map = defaultdict(list)
    for key_parts, value in pairs:
        key_map[key_sep.join(key_parts)].append(value)

    # Figure out the final dict with suffixes if required.
    for key, values in key_map.items():
        if len(values) == 1:  # No need to suffix keys.
            out[key] = values[0]
        else:  # More than one value for this key.
            for suffix, value in enumerate(values, 1):
                out[f'{key}{key_sep}{suffix}'] = value

    return out


# Parse XML with etree
tree = et.XML("""<?xml version="1.0"?>
<data>
    <country name="Liechtenstein">
        <rank>1</rank>
        <year>2008</year>
        <gdppc>141100</gdppc>
        <neighbor name="Austria" direction="E"/>
        <neighbor name="Switzerland" direction="W"/>
        <neighbor2 name="Italy" direction="S"/>
    </country>
    <country name="Singapore">
        <rank>4</rank>
        <year>2011</year>
        <gdppc>59900</gdppc>
        <neighbor name="Malaysia" direction="N"/>
        <cities>
            <city name="Chargin" population="1234" />
            <city name="Firin" population="4567" />
        </cities>
    </country>
    <country name="Panama">
        <rank>68</rank>
        <year>2011</year>
        <gdppc>13600</gdppc>
        <neighbor name="Costa Rica" direction="W"/>
        <neighbor name="Colombia" direction="E"/>
    </country>
</data>
""")


rows = [dictify_key_pairs(flatten_xml(row)) for row in tree]

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql import functions as F

spark = SparkSession.builder \
.master("local") \
.appName("Word Count") \
.getOrCreate()

df = spark.createDataFrame([{"a": "x", "b": "y", "c": "3","xml":rows}])

df.show()
+---+---+---+--------------------+
|  a|  b|  c|                 xml|
+---+---+---+--------------------+
|  x|  y|  3|[[neighbor-direct...|
+---+---+---+--------------------+

print(rows)
               
               
[{'name': 'Liechtenstein',
  'rank': '1',
  'year': '2008',
  'gdppc': '141100',
  'neighbor-name-1': 'Austria',
  'neighbor-name-2': 'Switzerland',
  'neighbor-direction-1': 'E',
  'neighbor-direction-2': 'W',
  'neighbor2-name': 'Italy',
  'neighbor2-direction': 'S'},
 {'name': 'Singapore',
  'rank': '4',
  'year': '2011',
  'gdppc': '59900',
  'neighbor-name': 'Malaysia',
  'neighbor-direction': 'N',
  'cities-city-name-1': 'Chargin',
  'cities-city-name-2': 'Firin',
  'cities-city-population-1': '1234',
  'cities-city-population-2': '4567'},
 {'name': 'Panama',
  'rank': '68',
  'year': '2011',
  'gdppc': '13600',
  'neighbor-name-1': 'Costa Rica',
  'neighbor-name-2': 'Colombia',
  'neighbor-direction-1': 'W',
  'neighbor-direction-2': 'E'}]
