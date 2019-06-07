package example;

import java.util.Arrays;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class WordCount {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "wordcount");
        JavaRDD<String> rdd = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> counts = WordCount.count(rdd);
        counts.saveAsTextFile(args[1]);
    }

    /**
     * @param stringRDD the RDD containing Strings to be word counted
     * @return A Tuple RDD key'd by the word with a value of the number of instances of the word. The returned
     * RDD is ordered by the count of the word in descending order.
     */
    static JavaPairRDD<String, Integer> count(JavaRDD<String> stringRDD) {
        return stringRDD
                .flatMap((String x) -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair((String x) -> new Tuple2<>(x, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2<String, Integer>::swap)
                .sortByKey(false)
                .mapToPair(Tuple2<Integer, String>::swap);
    }
}