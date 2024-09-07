import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object wordcount {
  def main(args: Array[String]): Unit = {
    val  
 sc = new SparkContext("local", "Word Count")

    // Read the input file as an RDD of lines
    val inputRDD = sc.textFile("fruits.txt")

    // Split each line into words and flatten the resulting RDD
    val wordsRDD = inputRDD.flatMap(_.split("\\s+"))

    // Map each word to a (word, 1) pair
    val wordPairsRDD = wordsRDD.map(word => (word, 1))

    // Reduce the word pairs by summing the counts
    val wordCountRDD = wordPairsRDD.reduceByKey(_ + _)

    // Print the top 10 words and their counts
    wordCountRDD.sortBy(_._2, ascending = false).take(10).foreach(println)

    sc.stop()
  }
}
