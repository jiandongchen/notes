# stack&pivot函数使用日记

## 背景

在做etl过程的时候，遇到一个比较奇特的需求，体验了之前极少使用的sparksql函数stack与pivot，在这边做一个记录。

需求是这样的：现在手上有一组json对象，其中每一个包含了某人的三次既往症信息。需要对其进行一维化满足hive存表的需求，结果表维度为一条既往症。

###### 原始json大概长这样：

![image](https://github.com/jiandongchen/notes/blob/main/summary/spark/images/json.jpg)

###### 产品的需求为：

![image](https://github.com/jiandongchen/notes/blob/main/summary/spark/images/requirement.png)

## 解决过程

1. ###### 首先对json进行拆解，对其一维化，结果如下，部分字段进行了省略

![image](https://github.com/jiandongchen/notes/blob/main/summary/spark/images/select-result.png)

这个过程十分简单，但需要注意的是目前的维度为一次请求，而我们想要的结果集的维度为一条既往症。目前的一条记录其实包含了潜在的三条记录，那很自然的想到，我们需要列转行。

2. ###### 列转行

   代码：

   ```scala
   import spark.implicits._
   
   val df = Seq(
     ("31834727", "1108", "5000", "7100", "7350", "7065", "5126",
   	"5719", "6321", "8484", "8484", "8484", "13", "11", "17"),
     ("31834814", "2106", "2000", "7105", "8170", "7070", "1417",
   	"1521", "1521", "8597", "8470", "8470", "238", "19", "19"),
     ("31834682", "6508", "6234", "7065", "7615", "7125", "1127",
   	"1717", "3909", "8888", "8557", "8996", "43", "25", "1954")
   ).toDF("id", "score_1", "score_2", "score_3", "score_4","score_5", "score_6", "score_7", "score_8", "score_9", "score_10","score_11", "score_12", "score_13", "score_14")
   
   val df1 = df
     .select(
   	'id,
   	expr(
   	  """
   		|stack(
   		| 12,
   		| 'score_3', score_3,
   		| 'score_4', score_4,
   		| 'score_5', score_5,
   		| 'score_6', score_6,
   		| 'score_7', score_7,
   		| 'score_8', score_8,
   		| 'score_9', score_9,
   		| 'score_10', score_10,
   		| 'score_11', score_11,
   		| 'score_12', score_12,
   		| 'score_13', score_13,
   		| 'score_14', score_14
   		|)
   		|as (score, value)
   		|""".stripMargin
   	)
     )
   ```

   运行结果：

   

   ![image](https://github.com/jiandongchen/notes/blob/main/summary/spark/images/df1.png)

   在列转行完成后，我们需要做的是再进行一次行转列，把需要的列转出来。我们需要的列是5列（id + score3、4、5对应的disease_code列 + score6、7、8对应的treatment_time列 + score9、10、11对应的hospital_code列 + score12、13、14对应的bill_amount列）。但这边存在一个问题，如果我们直接rename所有score列直接转的话，我们无法区分哪个disease_code对应哪个treatment_time了，因为这些字段其实是一一对应的。这边我们可以用一个小技巧来解决，添加一列symbol_for_disease，如果symbol_for_disease相同，那就表示一个即往症。

   3. ###### 行转列

      代码：

      ```scala
      val df2 = df1
      .withColumn(
        "symbol_for_disease",
        when('score.isin(Seq("score_3", "score_6", "score_9", "score_12"): _*), 1)
      	.when('score.isin(Seq("score_4", "score_7", "score_10", "score_13"): _*), 2)
      	.when('score.isin(Seq("score_5", "score_8", "score_11", "score_14"): _*), 3)
      )
        .where('symbol_for_disease.isNotNull)
        .na
        .replace(
      	"score",
      	Map(
      	  "score_3" -> "disease_code",
      	  "score_4" -> "disease_code",
      	  "score_5" -> "disease_code",
      	  "score_6" -> "treatment_time",
      	  "score_7" -> "treatment_time",
      	  "score_8" -> "treatment_time",
      	  "score_9" -> "hospital_code",
      	  "score_10" -> "hospital_code",
      	  "score_11" -> "hospital_code",
      	  "score_12" -> "bill_amount",
      	  "score_13" -> "bill_amount",
      	  "score_14" -> "bill_amount"
      	)
        )
        .groupBy('id, 'symbol_for_disease)
        .pivot("score")
        .agg(
      	first('value)
        )
        .drop("symbol_for_disease")
      ```

      运行结果：

      ![image](https://github.com/jiandongchen/notes/blob/main/summary/spark/images/df2.png)