package example;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;

public class DatasetWordCount {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Dataset Word Count").getOrCreate();
        Dataset<String> strings = spark.read().textFile(args[0]);
        Dataset<Row> orderedWordCount = orderedCount(strings);
        orderedWordCount.write().csv(args[1]);
    }

    public static Dataset<Row> orderedCount(Dataset<String> strings) {
        Dataset<String> words = strings.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING()
        );
        Dataset<Row> wordCount = words.groupBy("value").count().toDF("word", "count");
        Dataset<Row> orderedWordCount = wordCount.orderBy(functions.desc("count"));
        return orderedWordCount;
    }

}