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


def tuplify_city(file_rdd):
    tuple_2 = file_rdd.map(transform_string)
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
    filtered_cities = cities_file.filter(filter_city)
    mapped_cities = filtered_cities.map(filter_join_cities)
    mapped_regions = regions_file.map(filter_join_regions)
    joined_rdd = mapped_cities.join(mapped_regions)
    return joined_rdd.map(map_cities_regions)


def filter_join_cities(city_line):
    splitted = city_line.split(",")
    key = splitted[0].upper() + "," + splitted[3]
    return (key, city_line)


def filter_join_regions(region_line):
    splitted = region_line.split(",")
    key = splitted[0].upper() + "," + splitted[1]
    return (key, region_line)

def map_cities_regions(line):
    splitted_cities = line[1][0].split(",")
    splitted_regions = line[1][1].split(",")
    return (splitted_cities[1], splitted_regions[2])

if __name__ == "__main__":
    sc = SparkContext(CLUSTER, APP_NAME, pyFiles=[__file__])
    world_cities_file = sc.textFile("hdfs://" + sys.argv[1])
    region_codes_file = sc.textFile("hdfs://" + sys.argv[2])
    # valid_cities = tuplify_city(world_cities_file)
    # print("There is {:d} valid lines".format(valid_cities.count()))
    # print(cities_stats(valid_cities))
    # histogram_rdd = histogram(valid_cities)
    join_rdd = join(world_cities_file, region_codes_file)
    for line in join_rdd.take(10):
        print(line)
