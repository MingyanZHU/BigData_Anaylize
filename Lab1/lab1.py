from datetime import datetime
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import DenseVector
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession

longitude = [8.1461259, 11.1993265]
latitude = [56.5824856, 57.750511]
limit = []
coefficients = np.array([0.04071767126456369, -0.0003407797949423353, 0.004482420923481982, 3.1335151311401496e-05])

# with intercept
# Coefficients: [0.048019392959116204,-0.07314897932971658,0.004407404168702171,3.0984318016638766e-05]
# Intercept: 4.088178691783274


def parse(x):
    return x.split("|")


def filter_func(x):
    lng = float(x[1])
    lat = float(x[2])
    ans1 = longitude[0] <= lng <= longitude[1]
    ans2 = latitude[0] <= lat <= latitude[1]
    ans3 = x[6] == '?' or (limit[0] <= float(x[6]) <= limit[1])
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

    rating = "%.2f" % ((float(x[6]) - limit[0]) / (limit[1] - limit[0])) if x[6] != "?" else x[6]
    x[6] = rating
    return x


def parse_point(x):
    features = [float(x[1]), float(x[2]), float(x[3]), float(x[-1])]
    return LabeledPoint(float(x[6]), features)


def fill_rating(x):
    if x[6] != "?":
        return x
    else:
        _features = np.array([float(x[1]), float(x[2]), float(x[3]), float(x[-1])])
        return x[:6] + ["%.2f" % (_features.dot(coefficients))] + x[7:]


if __name__ == "__main__":
    rate = 0.01
    dic = {"doctor": rate, "programmer": rate, "teacher": rate, "farmer": rate,
           "artist": rate, "Manager": rate, "writer": rate, "accountant": rate}
    sc = SparkContext(appName="BigData Analyze Lab1")
    sc.setLogLevel("WARN")
    origin_record = sc.textFile("hdfs://localhost:9000/user/hadoop/input/large_data.txt").map(parse)
    seed = 3
    sample_record = origin_record.map(lambda x: (x[10], x)).sampleByKey(False, dic, seed=seed).map(lambda x: x[1])
    sample_record.map(lambda x: "|".join(x)).coalesce(1).saveAsTextFile("D_Sample")

    filtered_record_temp = sample_record.sortBy(lambda x: x[6]).collect()
    length = len(filtered_record_temp)
    outliers = length // 100
    limit = float(filtered_record_temp[outliers][6]), float(filtered_record_temp[-outliers][6])
    # print(limit)
    filtered_record = origin_record.filter(filter_func)
    filtered_record.map(lambda x: "|".join(x)).coalesce(1).saveAsTextFile("D_filtered")

    normalization_record = filtered_record.map(time_temp_parse)
    # normalization_record.map(lambda x: "|".join(x)).coalesce(1).saveAsTextFile("D_Normal")

    # Fill with missing values
    not_missing = dict(normalization_record.filter(lambda x: x[-1] != "?").map(
        lambda x: (x[-3] + " " + x[-2], float(x[-1]))).groupByKey().mapValues(list).map(
        lambda x: (x[0], "%.2f" % np.mean(x[1]))).collect())

    missing = normalization_record.filter(lambda x: x[-1] is "?").map(
        lambda x: x[:11] + [not_missing[x[-3] + " " + x[-2]]])
    # print(missing.collect())

    fill_missing_income = sc.union([missing, normalization_record.filter(lambda x: x[-1] != "?")])
    # fill_missing_income.saveAsTextFile("Fill_Income")

    # Not working using MLlib
    # parse_data = fill_missing_income.filter(lambda x: x[6] != "?").map(parse_point)
    # model = LinearRegressionWithSGD.train(parse_data, iterations=100, step=0.0001)
    # missing_rating = fill_missing_income.filter(lambda x: x[6] == "?").map(
    #     lambda x: x[:6] + [model.predict([float(x[1]), float(x[2]), float(x[3]), float(x[-1])])] + x[7:])
    # print(missing_rating.take(10))
    # values_and_preds = parse_data.map(lambda p: (p.label, model.predict(p.features)))
    # MSE = values_and_preds.map(lambda vp: (vp[0] - vp[1]) ** 2).reduce(lambda x, y: x + y) / values_and_preds.count()
    # print("Mean Squared Error = " + str(MSE))
    # print(values_and_preds.take(10))

    # Work with using ml
    # spark = SparkSession(sc)
    # training_data = spark.createDataFrame(fill_missing_income.filter(lambda x: x[6] != "?").map(
    #     lambda x: (float(x[6]), DenseVector([float(x[1]), float(x[2]), float(x[3]), float(x[-1])]))), ["label", "features"])
    # lr = LinearRegression(maxIter=10, regParam=0.2, elasticNetParam=0, fitIntercept=False)
    # lrModel = lr.fit(training_data)
    # print("Coefficients: %s" % str(lrModel.coefficients))
    # print("Intercept: %s" % str(lrModel.intercept))
    #
    # # Summarize the model over the training set and print out some metrics
    # trainingSummary = lrModel.summary
    # print("numIterations: %d" % trainingSummary.totalIterations)
    # print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
    # trainingSummary.residuals.show()
    # print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    # print("r2: %f" % trainingSummary.r2)
    fill_missing = fill_missing_income.map(fill_rating)
    fill_missing.map(lambda x: "|".join(x)).coalesce(1).saveAsTextFile("D_Preprocessed")
