import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
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
    .withColumn("doc_num", when($"serial_number".isNull, $"inn").otherwise($"serial_number"))
    .withColumn("doc_type", when($"serial_number".isNull, lit("ИНН")).otherwise(lit("Паспорт РФ")))
    .withColumn("validEmail", when($"email".rlike(emailRegex), $"email").otherwise(null))
    .withColumn("phones", concat_ws(";", $"phone0", $"phone1", $"phone3"))
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
      $"explodedPhones".alias("phone"),
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

  //Банк - Страховка
  //=======================================================================================================
  //..

  val bankToInsuranceDfPriority100 = preBankDf
    .join(preInsuranceDf.withColumn("priority_weight",lit(100)),
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
      $"priority_weight"
    )

  val bankToInsuranceDfPriority80 = preBankDf
    .join(preInsuranceDf.withColumn("priority_weight",lit(80)),
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
      $"priority_weight"
    )

  val bankToInsuranceDfPriority70 = preBankDf
    .join(preInsuranceDf.withColumn("priority_weight",lit(70)),
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
      $"priority_weight"
    )

  val bankToInsuranceDfPriority60 = preBankDf
    .join(preInsuranceDf.withColumn("priority_weight",lit(60)),
      preBankDf("fio") === preInsuranceDf("fio") &&
        preBankDf("dr") === preInsuranceDf("dr") &&
        regexp_replace(preBankDf("serial_number"), "\\s+", "") ===
          regexp_replace(preInsuranceDf("serial_number"), "\\s+", ""))
    .select(
      preBankDf("system_id").alias("bank_system_id"),
      preBankDf("client_id").alias("bank_client_id"),
      preInsuranceDf("system_id").alias("insurance_system_id"),
      preInsuranceDf("client_id").alias("insurance_client_id"),
      $"priority_weight"
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
    .select("*")
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
//    .show(1000, false)

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
    .select("*")
    .where("bank_client_id = 1001")
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
      .show(1000, false)

  //Граф
  //=======================================================================================================
  //..

//  case class BankInsEdge(bankClientId: Long, insClientId: Long, rules: Array[Int])
//  val bankInsEdges = Seq(
//    BankInsEdge(1, 1501, Array(100)),
//    BankInsEdge(2, 1502, Array(80)),
//  )
//  case class BankMarketEdge(bankClientId: Long, marketClientId: Long, rules: Array[Int])
//  val bankMarketEdges = Seq(
//    BankMarketEdge(3, 3003, Array(100)),
//    BankMarketEdge(4, 3004, Array(70)),
//  )
//
//  val bankVertices: RDD[(VertexId, String)] = bankClients.map(client =>
//    (s"1_${client.clientId}".hashCode.toLong, s"Bank_1_${client.clientId}")
//  )
//  val insVertices: RDD[(VertexId, String)] = insuranceClients.map(client =>
//    (s"2_${client.clientId}".hashCode.toLong, s"Insurance_1_${client.clientId}")
//  )
//  val marketVertices: RDD[(VertexId, String)] = marketClients.map(client =>
//    (s"3_${client.clientId}".hashCode.toLong, s"Market_1_${client.clientId}")
//  )
//
//  val allVertices = bankVertices.union(insVertices).union(marketVertices)
//
//  val bankInsEdgesRDD: RDD[Edge[Array[Int]]] = bankInsEdges.map { edge =>
//    val srcId = s"1_${edge.bankClientId}".hashCode.toLong
//    val dstId = s"2_${edge.insClientId}".hashCode.toLong
//    Edge(srcId, dstId, edge.rules)
//  }
//
//  val bankMarketEdgesRDD: RDD[Edge[Array[Int]]] = bankMarketEdges.map { edge =>
//    val srcId = s"1_${edge.bankClientId}".hashCode.toLong
//    val dstId = s"3_${edge.marketClientId}".hashCode.toLong
//    Edge(srcId, dstId, edge.rules)
//  }
//
//  val allEdges = bankInsEdgesRDD.union(bankMarketEdgesRDD)
//
//  val graph = Graph(allVertices, allEdges)
//
//  val connectedComponents = graph.connectedComponents().vertices
//
//  val clusters = connectedComponents.map { case (vertexId, clusterId) =>
//    (clusterId, vertexId)
//  }.groupByKey().map { case (clusterId, vertices) =>
//    (clusterId, vertices.toSet)
//  }
//
//  clusters.foreach { case (clusterId, vertexIds) =>
//    println(s"Cluster $clusterId contains vertices: ${vertexIds.mkString(", ")}")
//  }
//
//  val edgeRules = graph.edges.map(edge =>
//    (edge.srcId, edge.dstId, edge.attr)
//  )
//
//  case class ClusterResult(clusterId: Long, systemClientId: String, rules: Array[Int])
//  val clusterDF = clusters.flatMap { case (clusterId, vertices) =>
//    vertices.map(vertex => ClusterResult(clusterId, vertex.toString, Array())) // Дополнить правилами
//  }
//
//  clusterDF.write.parquet("clusters_output")

}