from flask import Flask,jsonify,json
from pyspark.sql import SparkSession
from flask import Response


app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

spark = SparkSession.builder.appName(
    'Read All CSV Files in Directory').getOrCreate()

file2 = spark.read.csv('/Users/naresh/PycharmProjects/stock_offlineProject/stock_values/*.csv', sep=',',
                       inferSchema=True, header=True)

file2.createOrReplaceTempView("table")

# #q9
@app.route('/highest_lowest_stock',methods=['GET'])
def question9():
    x = spark.sql("select Company, max(High) as maximum_price, min(Low) as minimun_price from  table group by Company")
    print(type(x))
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)

@app.route('/higer_avg_volume',methods=['GET'])
def question8():
    x=spark.sql(
        "select Company, avg(Volume) as average_volume from table group by Company order by average_volume desc limit 1")
    print(type(x))
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/avg_volume',methods=['GET'])
def question7():
    x=spark.sql("select AVG(Volume) as avgvolume from table  ")

    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/mean&medium_stock',methods=['GET'])
def question6():
    x=spark.sql("select Company,AVG(Open) from table group by Company ")

    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    y=spark.sql(
        "select distinct * from (select Company,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY Open) OVER (PARTITION BY Company) AS Median_UnitPrice from table)")
    results1 = y.toJSON().map(lambda j: json.loads(j)).collect()

    final={}
    final["mean"]=results
    final["median"]=results1

    return jsonify(final, 200)

@app.route('/standard_deviation_stock',methods=['GET'])
def question5():
    x=spark.sql("select Company,STDDEV(Open) as STD_OpenPrice from table group by Company ")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/max_move_tillnow',methods=['GET'])
def question4():
    x=spark.sql("select Company,Open,High,(High-Open) as max_diff from (Select Company, (Select Open from table limit 1) as Open, max(High) as High from table group by Company)table order by max_diff desc limit 1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/max_gap_from_previous_day',methods=['GET'])
def question3():
    x=spark.sql("with added_previous_close as (select Company,Open,Date,Close, " \
        "LAG(Close,1,35.724998) over(partition by Company order by Date) as previous_close " \
        "from table ASC) select Company,ABS(previous_close-Open) as max_swing from added_" \
        "previous_close order by max_swing DESC ")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/most_traded_stock',methods=['GET'])
def question2():
    x=spark.sql("WITH table2 AS( select Volume, Company,Date, DENSE_RANK() OVER(partition by Date order by volume) AS maximum from table) select * from table2 where maximum=1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/max_moved',methods=['GET'])
def question1():
    x=spark.sql("WITH table2 AS(select Date, Company, (High-Low)/Low, DENSE_RANK() OVER(partition by Date order by (High-Low)/Low desc) AS maximum from table) select * from table2 where maximum=1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

app.run(host='0.0.0.0', port=5008)