package edu.wisc.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCount{

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("WordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));
		JavaDStream<String> messages = ssc.textFileStream("/Users/rohitsd/services/spark-2.0.1-bin-hadoop2.7/test/");
		 // ssc.fileStream("/Users/rohitsd/services/spark-2.0.1-bin-hadoop2.7/test/", String.class, String.class, fClass)

		JavaDStream<String> words = messages.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String x) {
		        return Arrays.asList(SPACE.split(x)).iterator();
		      }
		    });
		
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
			      new PairFunction<String, String, Integer>() {
			        @Override
			        public Tuple2<String, Integer> call(String s) {
			          return new Tuple2<>(s, 1);
			        }
			      }).reduceByKey(
			        new Function2<Integer, Integer, Integer>() {
			        @Override
			        public Integer call(Integer i1, Integer i2) {
			          return i1 + i2;
			        }
			      });
			    wordCounts.print();
		// Get the lines, split them into words, count the words and print
//		JavaDStream<String> lines = messages.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
//			@Override
//			public Iterator<String> call(String x) {
//				return Arrays.asList(SPACE.split(x)).iterator();
//			}
//			@Override
//			public String call(Tuple2<String, String> tuple2) {
//				return tuple2._2();
//			}
//		});
//		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//			@Override
//			public Iterator<String> call(String x) {
//				return Arrays.asList(SPACE.split(x)).iterator();
//			}
//		});
		
		words.print();
		ssc.start();
		
		ssc.awaitTermination();
	}
}
