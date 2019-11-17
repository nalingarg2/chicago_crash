import sys
# import plotly.graph_objects as go
# import chart_studio.plotly as py
# import recertifi
import matplotlib.pyplot as plt

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
    from pyspark.sql.types import IntegerType
    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

def group_df(word):
    # print(int(word['AGE']), int(word['INJURIES_TOTAL']))
    # print(type(word['AGE']), type(word['INJURIES_TOTAL']))
    if int(word['AGE']) < 16:
        return (15, 1)
    elif int(word['AGE']) > 15 and int(word['AGE']) < 18:
        return (1617, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 17 and int(word['AGE']) < 20:
        return (1819, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 19 and int(word['AGE']) < 25:
        return (2024, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 24 and int(word['AGE']) < 30:
        return (2529, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 29 and int(word['AGE']) < 40:
        return (3039, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 39 and int(word['AGE']) < 50:
        return (4049, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 49 and int(word['AGE']) < 60:
        return (5059, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 59 and int(word['AGE']) < 70:
        return (6069, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 69 and int(word['AGE']) < 80:
        return (7079, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 79:
        return (80100, int(word['INJURIES_TOTAL']))


def main():
    spark = SparkSession.\
        builder.\
        appName(" Chicago Chrash Analysis").\
        getOrCreate()
    people_crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_People.csv",
                                     header=True)
    crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_Crashes.csv",
                                     header=True)
    vehicle_crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_Vehicles.csv",
                                     header=True)
    # print(people_crash_df.count())
    # print(crash_df.count())
    # print(vehicle_crash_df.count())
    # print(crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).count())

    # model vs fatal
    # crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).select('MODEL', 'INJURIES_FATAL',
    #                                                                                        'INJURIES_TOTAL').show()

    # accident vs age
    age_vs_accident = crash_df.join(people_crash_df, crash_df['RD_NO'] == people_crash_df['RD_NO']).select('AGE',
                                                                                           'INJURIES_TOTAL')
    # age_vs_accident.show()

    temp_df = age_vs_accident.where(age_vs_accident['INJURIES_TOTAL'] > 0).where(age_vs_accident["AGE"].isNotNull())

    int1_temp_df = temp_df.withColumn("AGE", temp_df["AGE"].cast(IntegerType()))
    int2_temp_df = int1_temp_df.withColumn("INJURIES_TOTAL", int1_temp_df["INJURIES_TOTAL"].cast(IntegerType()))
    # print(temp_df.rdd.collect(10))

    counts = int2_temp_df.rdd.map(lambda word: group_df(word)) \
        .reduceByKey(lambda a, b: a + b)
    print(counts.collect())
    df = counts.toDF()
    plot_test = df.toPandas().sort_values("_1", ascending=True)

    # plot_test = age_vs_accident.toPandas().sort_values(by = "AGE")
    plot_test.plot(figsize=(20, 10), kind="bar", color="red",
                                   x="_1", y="_2", legend=False, sort_columns=True)
    plt.xlabel("Age", fontsize=18)
    plt.ylabel("Count", fontsize=18)
    plt.title("Age Vs Accidents", fontsize=28)
    plt.xticks(size=18)
    plt.yticks(size=18)
    plt.show()

    # data = [go.Histogram(x=age_vs_accident.toPandas()['AGE'],
    #                      y=age_vs_accident.toPandas()['INJURIES_TOTAL'])]
    # py.iplot(data, filename="plot_age_vs_accident")


if __name__ == '__main__':
    main()
