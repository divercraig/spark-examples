package example;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;

public class WordCountTest extends SharedJavaSparkContext {

    @Test
    public void verifyWordCount() {
        List<String> input = Arrays.asList("cat dog bird", "dog bird", "bird");
        JavaRDD<String> inputRDD = jsc().parallelize(input);
        JavaPairRDD<String, Integer> result = WordCount.count(inputRDD);

        List<Tuple2<String, Integer>> expectedOutput = Arrays.asList(
                new Tuple2<>("bird", 3),
                new Tuple2<>("dog", 2),
                new Tuple2<>("cat", 1)
        );
        JavaPairRDD<String, Integer> expectedRDD = jsc().parallelizePairs(expectedOutput);

        ClassTag<Tuple2<String, Integer>> tag =
                scala.reflect.ClassTag$.MODULE$
                        .apply(Tuple2.class);


        JavaRDDComparisons.assertRDDEqualsWithOrder(
                JavaRDD.fromRDD(JavaPairRDD.toRDD(result), tag),
                JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag)
        );
    }
}