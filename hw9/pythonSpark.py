


data=sc.wholeTextFiles("hdfs:/loudacre/weblogs")
content=data.map(lambda x: x[1])
line=content.flatMap(lambda line: line.split("\n"))
htmls = line.filter(lambda x: "html" in x)
splits=htmls.map(lambda line: line.split(" "))
rdd = splits.map(lambda x: (x[2],1))
rdd = splits.map(lambda x: (x[2],1)).reduceByKey(lambda v1,v2: v1+v2)


data_2d = sc.textFile("hdfs:/loudacre/accounts")
line_2d = data_2d.map(lambda line: line.split(","))
result_2d = line_2d.map(lambda x: (x[0],x))
data_2e = result_2d.join(rdd)
result_2e=data_2e.map(lambda x: x[0]+" "+x[2]+" "+x[1][3]+" "+x[1][4])

result_2e.saveAsTextFile("/hw9test")


