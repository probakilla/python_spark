import sys
import math
from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics
from operator import add

NO_POP = -1
POP_COL = 4
REG_CODE_COL = 3
CSV_REG_CODE = 1
CITY_COL = 1
CLUSTER = "yarn"
APP_NAME = "python_spark"


def transform_string(city_string):
    splitted = city_string.split(",")
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
    valid_cities = tuplify_city(city_rdd)
    pop_rdd = valid_cities.map(lambda city: (
        int(math.log10(float(city[1]))), 1))
    return pop_rdd.reduceByKey(lambda accum, n: accum + n) \
                  .sortByKey() \
                  .map(lambda value: (int(math.pow(10, value[0])), value[1]))


def join(cities_file, regions_file):
    mapped_cities = cities_file.filter(filter_city) \
                               .map(mapper_join_cities)
    mapped_regions = regions_file.map(mapper_join_regions)
    return mapped_cities.join(mapped_regions) \
                        .reduceByKey(
                            lambda accum, x:
                            accum if accum[1][POP_COL] > x[1][POP_COL] else x) \
                        .map(map_cities_regions) \
                        .sortByKey()


def mapper_join_cities(city_line):
    splitted = city_line.split(",")
    key = splitted[0].upper() + "," + splitted[REG_CODE_COL]
    return (key, city_line)


def mapper_join_regions(region_line):
    splitted = region_line.split(",")
    key = splitted[0].upper() + "," + splitted[CSV_REG_CODE]
    return (key, region_line)


def map_cities_regions(line):
    splitted_cities = line[1][0].split(",")
    splitted_regions = line[1][1].split(",")
    return (splitted_cities[1], splitted_regions[2])


def main_join():
    sc = SparkContext(CLUSTER, APP_NAME, pyFiles=[__file__])
    num_executor = int(sc.getConf().get("spark.executor.instances"))
    world_cities_file = sc.textFile("hdfs://" + sys.argv[1],
                                    minPartitions=num_executor)
    region_codes_file = sc.textFile("hdfs://" + sys.argv[2],
                                    minPartitions=num_executor)
    joined_rdd = join(world_cities_file, region_codes_file)
    for line in joined_rdd.take(10):
        print(line)


def main_tuple_2():
    spark_context = SparkContext(CLUSTER, APP_NAME, pyFiles=[__file__])
    num_executor = int(spark_context.getConf().get("spark.executor.instances"))
    world_cities_file = spark_context.textFile("hdfs://" + sys.argv[1],
                                               minPartitions=num_executor)
    tuple_2 = tuplify_city(world_cities_file)
    for line in tuple_2.take(10):
        print(line)


def main_stats():
    spark_context = SparkContext(CLUSTER, APP_NAME, pyFiles=[__file__])
    num_executor = int(spark_context.getConf().get("spark.executor.instances"))
    world_cities_file = spark_context.textFile("hdfs://" + sys.argv[1],
                                               minPartitions=num_executor)
    tuplified_cities = tuplify_city(world_cities_file)
    print(cities_stats(tuplified_cities))


def main_hist():
    spark_context = SparkContext(CLUSTER, APP_NAME, pyFiles=[__file__])
    num_executor = int(spark_context.getConf().get("spark.executor.instances"))
    world_cities_file = spark_context.textFile("hdfs://" + sys.argv[1],
                                               minPartitions=num_executor)
    histogram_rdd = histogram(world_cities_file)
    for line in histogram_rdd.take(10):
        print(line)


if __name__ == "__main__":
    # main_join()
    # main_stats()
    main_hist()

# ex 30, cores 2, mem 2048M
# join : 1m11, 50s
# stats : 38s,
# hist : 41s


# ex 30, cores 4, mem 2048M
# join
# stats
# hist
