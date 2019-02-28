import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object WordCountByPos3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountByPos3")
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

    //一次shuffle
    val reduced: RDD[((String, String), Int)] = wordAndPosAndOne.reduceByKey(_+_)
    val posArray: Array[String] = reduced.map(_._1._1).distinct().collect()

    //自定义分区器
    val posPatitioner = new PosPatitioner(posArray)

    //按分区器分区，一次shuffle
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(posPatitioner)

    //对分区进行操作
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(_.toList.sortBy(_._2).reverse.take(3).iterator)

    //收集结果
    val result: Array[((String, String), Int)] = sorted.collect()

    //打印结果
    var key:String = ""
    result.map(posAndWordAndNum =>{
      if(posAndWordAndNum._1._1 != key){
        key = posAndWordAndNum._1._1
        println()
        println(key + ":")
      }
      print(posAndWordAndNum+" ")
    })

    sc.stop()
  }


}

class PosPatitioner(posArray: Array[String]) extends Partitioner{
  private val map = new mutable.HashMap[String, Int]()
  private var i = 0
  for(pos <- posArray){
    map.put(pos, i)
    i += 1
  }

  override def numPartitions: Int = posArray.length

  override def getPartition(key: Any): Int = {
    val pos: String = key.asInstanceOf[(String, String)]._1
    map(pos)
  }
}
