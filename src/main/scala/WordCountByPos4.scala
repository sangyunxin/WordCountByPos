import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object WordCountByPos4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountByPos4")
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

    val posArray: Array[String] = wordAndPosAndOne.map(_._1._1).distinct().collect()

    //自定义分区器
    val posPatitioner = new PosPatitioner(posArray)

    //一次shuffle
    val reduced: RDD[((String, String), Int)] = wordAndPosAndOne.reduceByKey(posPatitioner, _+_)

    //对分区进行操作
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      val arrayBuffer: ArrayBuffer[((String, String), Int)] = ArrayBuffer[((String, String), Int)]()
      while(it.hasNext){
        val cur: ((String, String), Int) = it.next()
        if(arrayBuffer.length < 3){
          arrayBuffer += cur
        }else{
          val min: ((String, String), Int) = arrayBuffer.minBy(_._2)
          if(cur._2 > min._2){
            arrayBuffer -= min
            arrayBuffer += cur
          }
        }
      }
      arrayBuffer.iterator
    })

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