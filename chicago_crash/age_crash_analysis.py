from visualization import visulation_df
from pyspark.sql.types import IntegerType
import pandas as pd


def group_df(word=None, driver_age_count=False):
    if driver_age_count:
        count = 1
    else:
        count = int(word['INJURIES_TOTAL'])
    if int(word['AGE']) < 16:
        return (15, count)
    elif int(word['AGE']) > 15 and int(word['AGE']) < 18:
        return (1617, count)

    elif int(word['AGE']) > 17 and int(word['AGE']) < 20:
        return (1819, count)

    elif int(word['AGE']) > 19 and int(word['AGE']) < 25:
        return (2024, count)

    elif int(word['AGE']) > 24 and int(word['AGE']) < 30:
        return (2529, count)

    elif int(word['AGE']) > 29 and int(word['AGE']) < 40:
        return (3039, count)

    elif int(word['AGE']) > 39 and int(word['AGE']) < 50:
        return (4049, count)

    elif int(word['AGE']) > 49 and int(word['AGE']) < 60:
        return (5059, count)

    elif int(word['AGE']) > 59 and int(word['AGE']) < 70:
        return (6069, count)

    elif int(word['AGE']) > 69 and int(word['AGE']) < 80:
        return (7079, count)

    elif int(word['AGE']) > 79:
        return (80100, count)


def age_crash_analysis(crash_df, people_crash_df):
    weight = _driver_age_count(people_crash_df)
    _age_crash(crash_df, people_crash_df, weight)


def _age_crash(crash_df, people_crash_df, weight):
    age_vs_accident = crash_df.join(people_crash_df, crash_df['RD_NO'] == people_crash_df['RD_NO']).select('AGE',
                                                                                                           'INJURIES_TOTAL')
    temp_df = age_vs_accident.where(age_vs_accident['INJURIES_TOTAL'] > 0).where(age_vs_accident["AGE"].isNotNull())
    int1_temp_df = temp_df.withColumn("AGE", temp_df["AGE"].cast(IntegerType()))
    int2_temp_df = int1_temp_df.withColumn("INJURIES_TOTAL", int1_temp_df["INJURIES_TOTAL"].cast(IntegerType()))
    counts = int2_temp_df.rdd.map(lambda word: group_df(word)) \
        .reduceByKey(lambda a, b: a + b)
    df = counts.toDF()
    plot_test = df.toPandas().sort_values("_1", ascending=True)
    weight_df = pd.DataFrame(weight, columns=['_1', '_3'])
    joined_df = plot_test.set_index("_1").join(weight_df.set_index("_1"))
    joined_df['weighted_column'] = joined_df['_2'] * joined_df['_3']
    visulation_df(df=plot_test, x_axis="_1", y_axis="_2",
                  x_label="Age", y_label="count",
                  title="Age Vs Accidents", sorting='x_axis')
    visulation_df(df=joined_df, x_axis="_1", y_axis="weighted_column",
                  x_label="Age", y_label="weighted_count",
                  title="weighted: Age Vs Accidents", sorting='x_axis')


def _driver_age_count(people_crash_df):
    age_count = people_crash_df.select('AGE').where(people_crash_df["AGE"].isNotNull()).\
        rdd.map(lambda word: group_df(word, True)).\
        reduceByKey(lambda a, b: a + b).collect()
    total_count = people_crash_df.select('AGE').where(people_crash_df["AGE"].isNotNull()).count()
    weight = map(lambda count: (count[0], count[1] / total_count), age_count)
    return weight