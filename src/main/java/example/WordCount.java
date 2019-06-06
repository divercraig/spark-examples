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
        JavaPairRDD<String, Integer> counts = rdd
                .flatMap((String x) -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair((String x) -> new Tuple2<>(x, 1))
                .reduceByKey(Integer::sum);
        counts.saveAsTextFile(args[1]);
    }
}