import numpy as np
from pyspark import SparkContext
from datetime import datetime

longitude = [8.1461259, 11.1993265]
latitude = [56.5824856, 57.750511]
limit = []


def parse(x):
    ans = x.split("|")
    return ans


def filter_func(x):
    lng = float(x[1])
    lat = float(x[2])
    ans1 = lng >= longitude[0] and lng <= longitude[1]
    ans2 = lat >= latitude[0] and lat <= latitude[1]
    ans3 = x[6] == '?' or (float(x[6]) >= limit[0] and float(x[6]) <= limit[1])
    return ans1 and ans2 and ans3

def reduce_rating(x):
    return 60 if x[6] == '?' else float(x[6])

def time_temp_parse(x):
    standard_time_format = "%Y-%m-%d"
    unstandard_format = "%Y/%m/%d"
    unstandard_format_2 = "%B %d,%Y"
    review_date = x[4]

    temperature = x[5]
    review_date_time = None
    if '/' in review_date:
        review_date_time = datetime.strptime(review_date, unstandard_format)
        review_date = review_date_time.strftime(standard_time_format)
    elif ',' in review_date:
        review_date_time = datetime.strptime(review_date, unstandard_format_2)
        review_date = review_date_time.strftime(standard_time_format)
    x[4] = review_date

    user_birthday = x[8]
    user_birthday_time = None
    if '/' in user_birthday:
        user_birthday_time = datetime.strptime(
            user_birthday, unstandard_format)
        user_birthday = user_birthday_time.strftime(standard_time_format)
    elif ',' in user_birthday:
        user_birthday_time = datetime.strptime(
            user_birthday, unstandard_format_2)
        user_birthday = user_birthday_time.strftime(standard_time_format)
    x[8] = user_birthday

    if '℉' in temperature:
        temperature = "%.1f" % (float(temperature[:-1]) - 32 / 1.8) + "℃"
        x[5] = temperature

    rating = "%.2f" %((float(x[6]) - limit[0])/(limit[1] - limit[0])) if x[6] != "?" else x[6]
    x[6] = rating
    return x


if __name__ == "__main__":
    rate = 0.01
    dic = {"doctor": rate, "programmer": rate, "teacher": rate, "farmer": rate,
           "artist": rate, "Manager": rate, "writer": rate, "accountant": rate}
    sc = SparkContext(appName="BigData Anaylize Lab1")
    sc.setLogLevel("WARN")
    origin_record = sc.textFile(
        "hdfs://localhost:9000/user/hadoop/input/large_data.txt").map(parse)
    seed = 3
    sample_record = origin_record.map(lambda x: (x[10], x)).sampleByKey(
        False, dic, seed=seed).map(lambda x: x[1])
    sample_record.map(lambda x: " ".join(x)).coalesce(1).saveAsTextFile("D_Sample")

    filtered_record_temp = sample_record.sortBy(lambda x: x[6]).collect()
    length = len(filtered_record_temp)
    outliers = length // 100
    limit = float(filtered_record_temp[outliers][6]), float(
        filtered_record_temp[-outliers][6])
    print(limit)
    filtered_record = origin_record.filter(filter_func)
    filtered_record.map(lambda x: " ".join(x)).coalesce(1).saveAsTextFile("D_filtered")

    normalization_record = filtered_record.map(time_temp_parse)
    normalization_record.map(lambda x: " ".join(x)).coalesce(1).saveAsTextFile("D_Normal")
