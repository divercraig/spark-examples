package example;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.sql.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DatasetWordCountTest extends SharedJavaSparkContext {

    @Test
    public void verifyDatasetWordCount() {
        List<String> input = Arrays.asList("cat dog bird", "dog bird", "bird");
        SparkSession spark = SparkSession.builder().getOrCreate();
        Dataset<String> inputDataset = spark.createDataset(input, Encoders.STRING());
        List<Row> result = DatasetWordCount.orderedCount(inputDataset).collectAsList();
        List<Row> expected = Arrays.asList(
                RowFactory.create("bird", 3),
                RowFactory.create("dog", 2),
                RowFactory.create("cat", 1)
        );
        Assert.assertEquals(expected, result);

    }
}
