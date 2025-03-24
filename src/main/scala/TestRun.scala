import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window

object TestRun extends App {
  val spark = SparkSession
    .builder()
    .appName("Курсовая работа Spark Developer")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  val sqlContext = spark.sqlContext

  import org.apache.spark.sql.functions._

  val sc = spark.sparkContext
  val emailRegex = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}"

  import spark.implicits._

  //Первичное чтение сгенерированных данных в DF
  //=======================================================================================================
  //..

  //Обработать имена от 3-х слов

  val df = spark.read.format("csv")
    .option("header", true)
    .option("delimiter", ";")
    .load("source.csv")

  val bank1 = df.filter(
        $"client_id".between(1,800) ||
        $"client_id".isin(1001))
    .withColumn("phones", array(array($"phone0",lit(0)),array($"phone1", lit(1)),array($"phone3", lit(2))))
    .withColumn("spacelessDul", regexp_replace($"serial_number", "\\s+", ""))
    .withColumn("validDul", concat_ws(" ",
      substring($"spacelessDul".cast("string"), 1, 2),
      substring($"spacelessDul".cast("string"), 3, 2),
      substring($"spacelessDul".cast("string"), 5, 8)
    ))
    .withColumn("validEmail", when($"email".rlike(emailRegex), $"email").otherwise(null))
    .select(
      $"client_id",
      $"fio",
      $"validDul".alias("dul"),
      $"dr",
      $"phones",
      $"validEmail".alias("email"))

  val insurance1 = df.filter(
        $"client_id".between(100,300) ||
        $"client_id".between(400,600) ||
        $"client_id".between(900,1000) ||
        $"client_id".isin(1000,1002,1006))
    .withColumn("new_id", $"client_id" + 1500)
    .withColumn("doc_num", when($"serial_number".isNull, $"inn").otherwise(regexp_replace($"serial_number", "\\s+", "")))
    .withColumn("doc_type", when($"serial_number".isNull, lit("ИНН")).otherwise(lit("Паспорт РФ")))
    .withColumn("validEmail", when($"email".rlike(emailRegex), $"email").otherwise(null))
    .withColumn("phones", concat_ws(";", coalesce($"phone0", lit("")), coalesce($"phone1", lit("")), coalesce($"phone3", lit("")))) //coalesce?
    .select(
      $"new_id".cast(IntegerType).alias("client_id"),
      $"fio".alias("full_name"),
      $"doc_num".alias("serial_number"),
      $"doc_type",
      $"dr",
      $"phones".alias("phone"),
      $"validEmail".alias("email"))

  val market1 = df.filter(
        $"client_id".between(800,900) ||
        $"client_id".between(200,700) ||
        $"client_id".isin(1003,1004,1005,1007))
    .withColumn("new_id", $"client_id" + 3000)
    .withColumn("name_parts", split($"fio", "\\s+"))
    .withColumn("first_name", element_at($"name_parts",2))
    .withColumn("surname", element_at($"name_parts",-1))
    .withColumn("last_name", element_at($"name_parts",1))
    .withColumn("validEmail", when($"email".rlike(emailRegex), $"email").otherwise(null))
    .select(
      $"new_id".cast(IntegerType).alias("client_id"),
      $"first_name",
      $"surname",
      $"last_name",
      $"phone0".alias("phone"),
      $"validEmail".alias("email"))
    //.where($"client_id" > 4000)

//  bank1.show(false)
//  insurance1.show(false)
//  market1.show(false)

  //Формирование исходной таблицы для матчинга
  //=======================================================================================================
  //..

  val explodedBank = bank1.withColumn("explodedPhones", explode($"phones"));
  val explodedInsurance = insurance1.withColumn("explodedPhones", explode(split(coalesce($"phone"), ";")))

  val preBankDf = explodedBank
    .withColumn("system_id", lit("Банк 1"))
    .select(
      $"system_id",
      $"client_id",
      $"fio",
      $"dr",
      $"dul".alias("serial_number"),
      $"explodedPhones".getItem(0).alias("phone"),
      $"explodedPhones".getItem(1).alias("phone_flag"),
      $"email")

  var preInsuranceDf = explodedInsurance
    .withColumn("system_id", lit("Страхование 1"))
    .select(
      $"system_id",
      $"client_id",
      $"full_name".alias("fio"),
      $"dr",
      $"serial_number",
      $"explodedPhones".alias("phone"), // Нужно ли обрабатывать телефоны?
      lit(null).alias("phone_flag"),
      $"email")

  var preMarketDf = market1
    .withColumn("system_id", lit("Маркет 1"))
    .select(
      $"system_id",
      $"client_id",
      concat_ws(" ", $"last_name", $"first_name", $"surname").alias("fio"),
      lit(null).alias("dr"),
      lit(null).alias("serial_number"),
      $"phone",
      lit(null).alias("phone_flag"),
      $"email")

  val final_unionized_df =
    preBankDf
      .unionAll(preInsuranceDf)
      .unionAll(preMarketDf)
      .where("fio = 'Бородач Александр Радионович'")
      //.show(false)

  //Банк - Страховка
  //=======================================================================================================
  //..

  val bankToInsuranceDfPriority100 = preBankDf
    .join(
      preInsuranceDf
          .withColumn("priority_weight",lit(100))
          .withColumn("rule_number", lit(1)),
        preBankDf("fio") === preInsuranceDf("fio") &&
        preBankDf("phone") === preInsuranceDf("phone") &&
        preBankDf("phone_flag") === 0 &&
        preBankDf("dr") === preInsuranceDf("dr") &&
        regexp_replace(preBankDf("serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("serial_number"), "\\s+", ""))
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preInsuranceDf("system_id").alias("insurance_system_id"),
      preInsuranceDf("client_id")alias("insurance_client_id"),
      $"priority_weight",
      $"rule_number"
    )

  val bankToInsuranceDfPriority80 = preBankDf
    .join(
      preInsuranceDf
        .withColumn("priority_weight",lit(80))
        .withColumn("rule_number", lit(2)),
      preBankDf("fio") === preInsuranceDf("fio") &&
        preBankDf("phone") === preInsuranceDf("phone") &&
        preBankDf("phone_flag") === 1 &&
        preBankDf("dr") === preInsuranceDf("dr") &&
        regexp_replace(preBankDf("serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("serial_number"), "\\s+", ""))
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preInsuranceDf("system_id").alias("insurance_system_id"),
      preInsuranceDf("client_id")alias("insurance_client_id"),
      $"priority_weight",
      $"rule_number"
    )

  val bankToInsuranceDfPriority70 = preBankDf
    .join(
      preInsuranceDf
        .withColumn("priority_weight",lit(70))
        .withColumn("rule_number", lit(3)),
        preBankDf("fio") === preInsuranceDf("fio") &&
        preBankDf("email") === preInsuranceDf("email") &&
        preBankDf("dr") === preInsuranceDf("dr") &&
        regexp_replace(preBankDf("serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("serial_number"), "\\s+", ""))
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preInsuranceDf("system_id").alias("insurance_system_id"),
      preInsuranceDf("client_id").alias("insurance_client_id"),
      $"priority_weight",
      $"rule_number"
    )

  val bankToInsuranceDfPriority60 = preBankDf
    .join(
      preInsuranceDf
        .withColumn("priority_weight",lit(60))
        .withColumn("rule_number", lit(4)),
        preBankDf("fio") === preInsuranceDf("fio") &&
        preBankDf("dr") === preInsuranceDf("dr") &&
        regexp_replace(preBankDf("serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("serial_number"), "\\s+", ""))
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preInsuranceDf("system_id").alias("insurance_system_id"),
      preInsuranceDf("client_id").alias("insurance_client_id"),
      $"priority_weight",
      $"rule_number"
    )

  val bankToInsuranceMatching = bankToInsuranceDfPriority100
    .unionAll(bankToInsuranceDfPriority80)
    .unionAll(bankToInsuranceDfPriority70)
    .unionAll(bankToInsuranceDfPriority60)
    .withColumn("max_weight", max("priority_weight")
      .over(Window.partitionBy(
        "bank_system_id",
        "bank_client_id",
        "insurance_system_id",
        "insurance_client_id")))
    .select($"*")
    .where("max_weight = priority_weight")
//    .groupBy(
//      "bank_system_id",
//      "bank_client_id",
//      "insurance_system_id",
//      "insurance_client_id"
//    )
//    .agg(
//      count("*").alias("countingDup") // Агрегатная функция count()
//    )
//    .where($"countingDup" > 1)

  //Банк - Меркет
  //=======================================================================================================
  //..

  val bankToMarketDfPriority100 = preBankDf
    .join(
      preMarketDf
          .withColumn("priority_weight",lit(100))
          .withColumn("rule_number", lit(1)),
        preBankDf("fio") === preMarketDf("fio") &&
        preBankDf("phone") === preMarketDf("phone") &&
        preBankDf("phone_flag") === 0 &&
        preBankDf("email") === preMarketDf("email"))
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preMarketDf("system_id").alias("market_system_id"),
      preMarketDf("client_id").alias("market_client_id"),
      $"priority_weight",
      $"rule_number"
    )

  val bankToMarketDfPriority80 = preBankDf
    .join(
      preMarketDf
          .withColumn("priority_weight",lit(80))
          .withColumn("rule_number", lit(2)),
        preBankDf("fio") === preMarketDf("fio") &&
        preBankDf("phone") === preMarketDf("phone") &&
        preBankDf("phone_flag") === 0)
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preMarketDf("system_id").alias("market_system_id"),
      preMarketDf("client_id").alias("market_client_id"),
      $"priority_weight",
      $"rule_number"
    )

  val bankToMarketDfPriority70 = preBankDf
    .join(
      preMarketDf
          .withColumn("priority_weight",lit(70))
          .withColumn("rule_number", lit(3)),
        preBankDf("fio") === preMarketDf("fio") &&
        preBankDf("email") === preMarketDf("email"))
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preMarketDf("system_id").alias("market_system_id"),
      preMarketDf("client_id").alias("market_client_id"),
      $"priority_weight",
      $"rule_number"
    )

  val bankToMarketMatching = bankToMarketDfPriority100
    .unionAll(bankToMarketDfPriority80)
    .unionAll(bankToMarketDfPriority70)
    .withColumn("max_weight", max("priority_weight")
      .over(Window.partitionBy(
        "bank_system_id",
        "bank_client_id",
        "market_system_id",
        "market_client_id")))
    .select("*")
    .where("max_weight = priority_weight")
//      .groupBy(
//        "bank_system_id",
//        "bank_client_id",
//        "market_system_id",
//        "market_client_id"
//      )
//      .agg(
//        count("*").alias("countingDup") // Агрегатная функция count()
//      )
//      .where($"countingDup" > 1)
//      .show(1000, false)

  //Граф
  //=======================================================================================================
  //..

  val bankToInsuranceRenamed = bankToInsuranceMatching
    .select(
      $"bank_system_id",
      $"bank_client_id",
      $"insurance_system_id".as("partner_system_id"),
      $"insurance_client_id".as("partner_client_id"),
      $"rule_number", $"priority_weight")

  val bankToMarketRenamed = bankToMarketMatching
    .select(
      $"bank_system_id",
      $"bank_client_id",
      $"market_system_id".as("partner_system_id"),
      $"market_client_id".as("partner_client_id"),
      $"rule_number", $"priority_weight")

  //bankToMarketRenamed.select("*").where("bank_client_id = 1001").show()

  val allEdgesDF = bankToInsuranceRenamed.unionAll(bankToMarketRenamed)
    .withColumn("src", $"bank_client_id".cast("long"))
    .withColumn("dst", $"partner_client_id".cast("long"))
    .withColumn("rule_arr", array($"rule_number"))
    .select("src", "dst", "rule_arr")

  val edgesRDD: RDD[Edge[Array[Int]]] = allEdgesDF.rdd.map(row => {
    val srcId = row.getAs[Long]("src")
    val dstId = row.getAs[Long]("dst")
    val rules = row.getAs[Seq[Int]]("rule_arr").toArray
    Edge(srcId, dstId, rules)
  })

  val verticesRDD: RDD[(VertexId, String)] = allEdgesDF
    .select("src")
    .union(allEdgesDF.select("dst"))
    .rdd
    .map(row => {
      val id = row.getAs[Long](0)
      //println((id, s"Client_$id"))
      (id, s"Client_$id")
    })

  val graph = Graph(verticesRDD, edgesRDD)

  val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices

  val ccDF = connectedComponents.toDF("client_id", "cluster_id")

  //ccDF.select("*").where("cluster_id = 1001").show()

  val bankToInsuranceResult = bankToInsuranceRenamed
    .withColumn("bank_client_id_long", $"bank_client_id".cast("long"))
    .join(ccDF, $"bank_client_id_long" === ccDF("client_id"), "left")
    .show()

  val bankToMarketResult = bankToInsuranceRenamed
    .withColumn("bank_client_id_long", $"bank_client_id".cast("long"))
    .join(ccDF, $"bank_client_id_long" === ccDF("client_id"), "left")
    .show()

}