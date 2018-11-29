import sys
import math
from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics

NO_POP = -1
POP_COL = 4
CITY_COL = 1
CLUSTER = "yarn-cluster"
APP_NAME = "python_spark"


def transform_string(cityString):
    splitted = cityString.split(",")
    valid_pop = (splitted[POP_COL] and splitted[POP_COL].isdigit())
    pop = NO_POP if not valid_pop else splitted[POP_COL]
    return (splitted[CITY_COL], pop)


def open_hdfs(file):
    ''' Retrieves a RDD of the worldcitiespop.original file '''
    sc = SparkContext(CLUSTER, APP_NAME, pyFiles=[__file__])
    return sc.textFile("hdfs://" + file)


def tuplify_city(file):
    rdd_file = open_hdfs(file)
    tuple_2 = rdd_file.map(transform_string)
    valid = tuple_2.filter(
        lambda city: city[CITY_COL] != NO_POP)
    return valid

def filter_city(line):
    splitted = line.split(",")
    return splitted[POP_COL] and splitted[POP_COL].isdigit()


def cities_stats(city_rdd):
    pop_rdd = city_rdd.map(lambda city: [city[1]])
    statistics = Statistics.colStats(pop_rdd)
    mean = statistics.mean()[0]
    variance = statistics.variance()[0]
    max_pop = statistics.max()[0]
    min_pop = statistics.min()[0]
    res = {
        "mean": mean,
        "variance": variance,
        "deviation": math.sqrt(variance),
        "max": max_pop,
        "min": min_pop
    }
    return res


def histogram(city_rdd):
    pop_rdd = city_rdd.map(lambda city: (int(math.log10(float(city[1]))), 1))
    return pop_rdd.reduceByKey(lambda accum, n: accum + n) \
                  .sortByKey() \
                  .map(lambda value: (int(math.pow(10, value[0])), value[1]))

def join(cities_file, regions_file):
    cities_rdd = open_hdfs(cities_file)
    regions_rdd = open_hdfs(regions_file)
    filtered_cities = cities_rdd.filter(filter_city)


if __name__ == "__main__":
    valid_cities = tuplify_city(sys.argv[1])
    # print("There is {:d} valid lines".format(valid_cities.count()))
    # print(cities_stats(valid_cities))
    histogram_rdd = histogram(valid_cities)
    for line in histogram_rdd.collect():
        print(line)
