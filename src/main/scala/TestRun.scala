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
    .select(
      $"client_id",
      $"fio",
      $"validDul".alias("dul"),
      $"dr",
      $"phones",
      $"email")

  val insurance1 = df.filter(
        $"client_id".between(100,300) ||
        $"client_id".between(400,600) ||
        $"client_id".between(900,1000) ||
        $"client_id".isin(1000,1002,1006))
    .withColumn("new_id", $"client_id" + 1500)
    .withColumn("doc_num", when($"serial_number".isNull, $"inn").otherwise(regexp_replace($"serial_number", "\\s+", "")))
    .withColumn("doc_type", when($"serial_number".isNull, lit("ИНН")).otherwise(lit("Паспорт РФ")))
    .withColumn("phones", trim(concat_ws(" ", coalesce($"phone0", lit("")), coalesce($"phone1", lit("")), coalesce($"phone3", lit(""))))) //coalesce?
    .select(
      $"new_id".cast(IntegerType).alias("client_id"),
      $"fio".alias("full_name"),
      $"doc_num".alias("serial_number"),
      $"doc_type",
      $"dr",
      $"phones".alias("phone"),
      $"email")

  val market1 = df.filter(
        $"client_id".between(800,900) ||
        $"client_id".between(200,700) ||
        $"client_id".isin(1003,1004,1005,1007))
    .withColumn("new_id", $"client_id" + 3000)
    .withColumn("name_parts", split($"fio", "\\s+"))
    .withColumn("first_name", element_at($"name_parts",2))
    .withColumn("surname", element_at($"name_parts",-1))
    .withColumn("last_name", element_at($"name_parts",1))
    .select(
      $"new_id".cast(IntegerType).alias("client_id"),
      $"first_name",
      $"surname",
      $"last_name",
      //coalesce($"phone0",$"phone1").alias("phone"),
      $"phone0".alias("phone"),
      $"email")
    //.where($"client_id" > 4000)

//  bank1.show(false)
//  insurance1.show(false)
//  market1.show(false)

  //Формирование исходной таблицы для матчинга
  //=======================================================================================================
  //..

  val explodedBank = bank1.withColumn("explodedPhones", explode(filter($"phones", x =>
    x.getItem(0).isNotNull && x.getItem(1).isNotNull)));
  val explodedInsurance = insurance1.withColumn("explodedPhones", explode(split($"phone", " ")))

  val preBankDf = explodedBank
    .withColumn("system_id", lit("Банк 1"))
    .withColumn("valid_name",
        when(size(split($"fio", "\\s+")) > 3, lit(null))
        otherwise($"fio"))
    .withColumn("valid_email",
        when($"email".rlike(emailRegex), $"email")
        otherwise(lit(null)))
    .select(
      $"system_id".alias("bank_system_id"),
      $"client_id".alias("bank_client_id"),
      $"valid_name".alias("bank_fio"),
      $"dr".alias("bank_dr"),
      $"dul".alias("bank_serial_number"),
      $"explodedPhones".getItem(0).alias("bank_phone"),
      $"explodedPhones".getItem(1).alias("bank_phone_flag"),
      $"valid_email".alias("bank_email"))

  var preInsuranceDf = explodedInsurance
    .withColumn("system_id", lit("Страхование 1"))
    .withColumn("valid_name",
        when(size(split($"full_name", "\\s+")) > 3, lit(null))
        otherwise($"full_name"))
    .withColumn("valid_email",
        when($"email".rlike(emailRegex), $"email")
        otherwise(lit(null)))
    .select(
      $"system_id".alias("insurance_system_id"),
      $"client_id".alias("insurance_client_id"),
      $"valid_name".alias("insurance_fio"),
      $"dr".alias("insurance_dr"),
      $"serial_number".alias("insurance_serial_number"),
      $"explodedPhones".alias("insurance_phone"), // Нужно ли обрабатывать телефоны?
      lit(null).alias("insurance_phone_flag"),
      $"valid_email".alias("insurance_email"))

  var preMarketDf = market1
    .withColumn("system_id", lit("Маркет 1"))
    .withColumn("full_name", concat_ws(" ", $"last_name", $"first_name", $"surname"))
    .withColumn("valid_name",
        when(size(split($"full_name", "\\s+")) > 3, lit(null))
        otherwise($"full_name"))
    .withColumn("valid_email",
        when($"email".rlike(emailRegex), $"email")
        otherwise(lit(null)))
    .select(
      $"system_id".alias("market_system_id"),
      $"client_id".alias("market_client_id"),
      $"valid_name".alias("market_fio"),
      lit(null).alias("market_dr"),
      lit(null).alias("market_serial_number"),
      $"phone".alias("market_phone"),
      lit(null).alias("market_phone_flag"),
      $"valid_email".alias("market_email"))

  val final_unionized_df =
    preBankDf
      .select(
        $"bank_system_id".alias("system_id"),
        $"bank_client_id".alias("client_id"),
        $"bank_fio".alias("fio"),
        $"bank_dr".alias("dr"),
        regexp_replace($"bank_serial_number", "\\s+", "").alias("serial_number"),
        $"bank_phone".alias("phone"),
        $"bank_phone_flag".alias("phone_flag"),
        $"bank_email".alias("email")
      )
      .unionAll(preInsuranceDf)
      .unionAll(preMarketDf)
      .show(false)

  //Банк - Страховка
  //=======================================================================================================
  //..

  val bankToInsuranceDfPriority100 = preBankDf
    .join(
      preInsuranceDf
          .withColumn("priority_weight",lit(100))
          .withColumn("rule_number", lit(1)),
        preBankDf("bank_fio") === preInsuranceDf("insurance_fio") &&
        preBankDf("bank_phone") === preInsuranceDf("insurance_phone") &&
        preBankDf("bank_phone_flag") === 0 &&
        preBankDf("bank_dr") === preInsuranceDf("insurance_dr") &&
        regexp_replace(preBankDf("bank_serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("insurance_serial_number"), "\\s+", ""))
    .select("*")

  val bankToInsuranceDfPriority80 = preBankDf
    .join(
      preInsuranceDf
        .withColumn("priority_weight",lit(80))
        .withColumn("rule_number", lit(2)),
        preBankDf("bank_fio") === preInsuranceDf("insurance_fio") &&
        preBankDf("bank_phone") === preInsuranceDf("insurance_phone") &&
        preBankDf("bank_phone_flag") === 1 &&
        preBankDf("bank_dr") === preInsuranceDf("insurance_dr") &&
        regexp_replace(preBankDf("bank_serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("insurance_serial_number"), "\\s+", ""))
    .select("*")

  val bankToInsuranceDfPriority70 = preBankDf
    .join(
      preInsuranceDf
        .withColumn("priority_weight",lit(70))
        .withColumn("rule_number", lit(3)),
        preBankDf("bank_fio") === preInsuranceDf("insurance_fio") &&
        preBankDf("bank_email") === preInsuranceDf("insurance_email") &&
        preBankDf("bank_dr") === preInsuranceDf("insurance_dr") &&
        regexp_replace(preBankDf("bank_serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("insurance_serial_number"), "\\s+", ""))
    .select("*")

  val bankToInsuranceDfPriority60 = preBankDf
    .join(
      preInsuranceDf
        .withColumn("priority_weight",lit(60))
        .withColumn("rule_number", lit(4)),
        preBankDf("bank_fio") === preInsuranceDf("insurance_fio") &&
        preBankDf("bank_dr") === preInsuranceDf("insurance_dr") &&
        regexp_replace(preBankDf("bank_serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("insurance_serial_number"), "\\s+", ""))
    .select("*")

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
        preBankDf("bank_fio") === preMarketDf("market_fio") &&
        preBankDf("bank_phone") === preMarketDf("market_phone") &&
        preBankDf("bank_phone_flag") === 0 &&
        preBankDf("bank_email") === preMarketDf("market_email"))
    .select("*")

  val bankToMarketDfPriority80 = preBankDf
    .join(
      preMarketDf
          .withColumn("priority_weight",lit(80))
          .withColumn("rule_number", lit(2)),
        preBankDf("bank_fio") === preMarketDf("market_fio") &&
        preBankDf("bank_phone") === preMarketDf("market_phone") &&
        preBankDf("bank_phone_flag") === 0)
    .select("*")

  val bankToMarketDfPriority70 = preBankDf
    .join(
      preMarketDf
          .withColumn("priority_weight",lit(70))
          .withColumn("rule_number", lit(3)),
        preBankDf("bank_fio") === preMarketDf("market_fio") &&
        preBankDf("bank_email") === preMarketDf("market_email"))
    .select("*")

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

  val dfForConfirmation = bankToInsuranceMatching
    .select(
      $"bank_system_id".alias("system_id"),
      $"bank_client_id".alias("client_id"),
      $"bank_fio".alias("fio"),
      $"bank_dr".alias("dr"),
      regexp_replace($"bank_serial_number", "\\s+", "").alias("serial_number"),
      $"bank_phone".alias("phone"),
      $"bank_phone_flag".alias("phone_flag"),
      $"bank_email".alias("email")
    )
    .unionAll(
      bankToInsuranceMatching.select(
          $"insurance_system_id".alias("system_id"),
          $"insurance_client_id".alias("client_id"),
          $"insurance_fio".alias("fio"),
          $"insurance_dr".alias("dr"),
          $"insurance_serial_number".alias("serial_number"),
          $"insurance_phone".alias("phone"),
          $"insurance_phone_flag".alias("phone_flag"),
          $"insurance_email".alias("email")
        )
    )
    .unionAll(
      bankToMarketMatching.select(
        $"market_system_id".alias("system_id"),
        $"market_client_id".alias("client_id"),
        $"market_fio".alias("fio"),
        $"market_dr".alias("dr"),
        $"market_serial_number".alias("serial_number"),
        $"market_phone".alias("phone"),
        $"market_phone_flag".alias("phone_flag"),
        $"market_email".alias("email")
      )
    ).distinct()

  dfForConfirmation
    .withColumn("ins_1", md5(concat($"fio", when(($"phone_flag" === 0), $"phone").otherwise(lit(null)), $"dr", $"serial_number")))
    .withColumn("ins_2", md5(concat($"fio", when(($"phone_flag" === 1), $"phone").otherwise(lit(null)), $"dr", $"serial_number")))
    .withColumn("ins_3", md5(concat($"fio", $"email", $"dr", $"serial_number")))
    .withColumn("ins_4", md5(concat($"fio", $"dr", $"serial_number")))
    .withColumn("shop_1", md5(concat($"fio", when(($"phone_flag" === 0), $"phone").otherwise(lit(null)), $"email")))
    .withColumn("shop_2", md5(concat($"fio", when(($"phone_flag" === 0), $"phone").otherwise(lit(null)))))
    .withColumn("shop_3", md5(concat($"fio", $"email")))
    .select("*")
    //.where("fio = 'Бородач Александр Радионович'")
    .show(1000)


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

//  ccDF.select("*").where("cluster_id = 1001").show()

  val bankToInsuranceResult = bankToInsuranceRenamed
    .join(ccDF, $"bank_client_id" === ccDF("client_id"), "left")
    .groupBy($"bank_system_id",
              $"bank_client_id",
              $"partner_system_id",
              $"partner_client_id",
              $"cluster_id")
    .agg(collect_set("rule_number").as("rule_set"))
    .show()

  val bankToMarketResult = bankToMarketRenamed
    .join(ccDF, $"bank_client_id" === ccDF("client_id"), "left")
    .groupBy($"bank_system_id",
              $"bank_client_id",
              $"partner_system_id",
              $"partner_client_id",
              $"cluster_id")
    .agg(collect_set("rule_number").as("rule_set"))
    .show()

}