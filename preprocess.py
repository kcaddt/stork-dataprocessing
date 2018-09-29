from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, collect_set, udf, count, countDistinct
from pyspark.sql.types import BooleanType, ArrayType, StringType

import json, numpy

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

def renameDF(df, colnames):
    for i in colnames.keys():
        df = df.withColumnRenamed('_c%s'%i, colnames[i]['id'])
        df = df.withColumn(
                            colnames[i]['id'],
                            col(colnames[i]['id']).cast(colnames[i]['type'])
                          )
    return df

def limitPostalCode(arr):
    return len(arr) <= 10

def stringToArray(string):
    if string: return string.split(',')
    return string

limitPostalCode = udf(limitPostalCode, BooleanType())
stringToArray = udf(stringToArray, ArrayType(StringType()))

columns =  {
                'cities' : {
                    0 : {'id' : 'geonameid', 'type' : 'int'},
                    1 : {'id' : 'name', 'type' : 'string'},
                    3 : {'id' : 'alternativeNames', 'type' : 'string'},
                    4 : {'id' : 'lat', 'type' : 'double'},
                    5 : {'id' : 'lng', 'type' : 'double'},
                    8 : {'id' : 'countryCode', 'type' : 'string'},
                    14 : {'id' : 'population', 'type' : 'int'}
                },
                'postcodes' : {
                    0 : {'id' : 'alternateNameId', 'type' : 'int'},
                    1 : {'id' : 'geonameid', 'type' : 'int'},
                    2 : {'id' : 'isolang', 'type' : 'string'},
                    3 : {'id' : 'alternateName', 'type' : 'string'},
                    4 : {'id' : 'isPreferredName', 'type' : 'int'}
                }
            }


ct = spark.read.csv('inputs/cities1000.txt', sep='\t')\
          .select(*['_c%s' % i for i in list(columns['cities'].keys())])
an = spark.read.csv('inputs/alternateNamesV2.txt', sep='\t')\
          .select(*['_c%s' % i for i in list(columns['postcodes'].keys())])

countryCode = spark.read.csv('inputs/iso-3166.csv', header=True)\
                   .select(col('name').alias('country'), col('alpha-2').alias('countryCode'))


ct = renameDF(ct, columns['cities'])
ct = ct.join(countryCode,'countryCode')

postalCode = renameDF(an, columns['postcodes']).filter(col('isolang') == "post")

postalCodePrefered = postalCode.filter(col('isPreferredName') == 1)\
                               .groupBy('geonameid')\
                               .agg(collect_set(col('alternateName')).alias('postalCode'))

"""
postalCode.groupBy('geonameid')\
          .agg(countDistinct('alternateName').alias('nPC'))\
          .groupBy('nPC').agg(count('geonameid')).sort(col('nPC')).show()


The distribution of the number of the postal codes show that
less than 0.4% of the  geonameids have more than 10 postal codes
so I choose to keep only the one with less than 10 postal codes
"""

otherPostalCodeAggregated = postalCode.join(postalCodePrefered, 'geonameid', 'leftanti')\
                                      .groupBy('geonameid')\
                                      .agg(collect_set('alternateName').alias('postalCode'))\
                                      .filter(limitPostalCode('postalCode'))\
                                      .select('geonameid', col('postalCode'))

cleanedPostalCode = postalCodePrefered.union(otherPostalCodeAggregated)

cities = ct.join(cleanedPostalCode, 'geonameid', 'left')\
           .select(*['geonameid','name',  'alternativeNames', 'lat', 'lng', 'population', 'postalCode', 'country'])\
           .withColumnRenamed('geonameid', 'objectID')
cities_json = cities.toJSON().map(lambda e: json.loads(e)).collect()

for e in cities_json :

    e["_geoloc"] = {
        "lat" : e["lat"],
        "lng" : e["lng"],
    }

    if "postalCode" not in e :
        e["postalCode"] = None

"""
We split the files for the bulk
"""
files = [L.tolist() for L in numpy.array_split(cities_json, 6)]

for idx, file in enumerate(files):
    with open('outputs/cities00%s.json' % idx, 'w') as outfile:
        json.dump(file, outfile)
