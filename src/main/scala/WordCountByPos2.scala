import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object WordCountByPos2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountByPos2")
    val sc = new SparkContext(conf)

    val inputFile: String = "hdfs://spark.sangyx.cn:9000/wordcount/test_pos.txt"
    val textFile: RDD[String] = sc.textFile(inputFile)

    val wordAndPosAndOne: RDD[((String, String), Int)] = textFile.flatMap(line => {
      val replaced: String = line.toLowerCase().replaceAll("( ,)|( ;)|( \\.)", "")
      val splited: Array[String] = replaced.split(" ")
      val wordAndPosAndOne: Array[((String, String), Int)] = splited.map(wordAndPos => {
        val index: Int = wordAndPos.indexOf("/")
        val word: String = wordAndPos.substring(0, index)
        val pos: String = wordAndPos.substring(index + 1)
        ((pos, word), 1)
      })
      wordAndPosAndOne
    })

    val reduced: RDD[((String, String), Int)] = wordAndPosAndOne.reduceByKey(_+_)
    val posArray: Array[String] = reduced.keys.map(_._1).distinct().collect()

    for(pos <- posArray){
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == pos)
      val result: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(3)
      println(pos + ":")
      println(result.mkString(" "))
    }

    sc.stop()
  }

}
