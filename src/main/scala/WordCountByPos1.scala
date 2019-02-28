import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountByPos1 {

  def main(args: Array[String]): Unit = {
    // 设置spark环境
    val conf = new SparkConf().setAppName("WordCountByPos1")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // 读取输入文件
    val inputFile = "hdfs://spark.sangyx.cn:9000/wordcount/test_pos.txt"
    val textFile: RDD[String] = sc.textFile(inputFile)

    val wordAndPosAndOne: RDD[((String, String), Int)] = textFile.flatMap(line => {
      //转为小写，去除标点符号
      val replaced: String = line.toLowerCase().replaceAll("( ,)|( ;)|( \\.)", "")
      //切分句子,结果是(wordAndPos, .., wordAndPos)
      val splited: Array[String] = replaced.split(" ")

      val wordAndPosAndOne: Array[((String, String), Int)] = splited.map(wordAndPos => {
        //切分word和pos
        val index: Int = wordAndPos.indexOf("/")
        val word: String = wordAndPos.substring(0, index)
        val pos: String = wordAndPos.substring(index + 1)
        ((pos, word), 1)
      })
      wordAndPosAndOne
    })

    //以(pos,word)为key进行聚合
    val reduced: RDD[((String, String), Int)] = wordAndPosAndOne.reduceByKey(_ + _)

    //以pos为key将数据分组
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    //对每组内的数据根据数量进行排序
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    //收集结果
    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    //打印
    result.foreach(r => {
      println(r._1)
      println(r._2)
    })

    // 结束
    sc.stop()
  }
}
