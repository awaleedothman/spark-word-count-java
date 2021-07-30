import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("input");
        JavaPairRDD<String, Integer> words = rdd
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word.replaceAll("[^a-zA-Z]", "").toLowerCase(), 1))
                .reduceByKey(Integer::sum);
        words.saveAsTextFile("output");
    }
}
