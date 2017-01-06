import org.apache.spark.sql.SparkSession

object MyWordCount {
  def main(args: Array[String]) {

    //获取SparkContext
    val spark = SparkSession
      .builder
      .appName("Spark Pi").master("local")
      .getOrCreate()

    var sc = spark.sparkContext

    //读取文件，返回这个文件的行数
    val count = sc.textFile("F:\\vmware\\share\\soft\\spark-1.6.0-bin-hadoop2.6\\README.md").count()
    println(count)

    //    val lines = spark.sparkContext.textFile("F:\\vmware\\share\\soft\\spark-1.6.0-bin-hadoop2.6\\README.md")
    //    val wordcount = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).collect()
    //    wordcount.foreach(pair => println(pair._1 + "  :  " + pair._2))

    /**
      * flatMap产生 MapPartitionsRDD
      * map 产生 MapPartitionsRDD
      * reduceByKey 产生 ShuffledRDD
      * sortByKey 产生 ShuffledRDD
      */
    spark.sparkContext.textFile("F:\\vmware\\share\\soft\\spark-1.6.0-bin-hadoop2.6\\README.md").flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey(_+_).map(pair => (pair._2, pair._1)).sortByKey(false).collect()
      .map(pair => (pair._2, pair._1)).foreach(pair => println(pair._1 + "  :  " + pair._2))

    //为了可以通过web控制台看到信息，加一个写循环不让程序结束
    while (true) {}


    spark.stop()

  }
}