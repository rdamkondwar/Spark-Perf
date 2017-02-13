package edu.wisc.streaming;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import edu.wisc.streaming.util.RandomWordReceiver;
import scala.Tuple2;

public final class RandomWordCount implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: RandomWordCount <batchSize>");
			System.exit(1);
		}

		Long batchDuration = Long.parseLong(args[0]);

		SparkConf sparkConf = new SparkConf().setAppName("JRandomWordCount");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(batchDuration));

		// jssc.checkpoint("/tmp/services/checkpoint");

		JavaDStream<String> messages = jssc.receiverStream(new RandomWordReceiver());

		JavaPairDStream<String, Integer> wordCounts = messages.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.print();

		// Start the computation
		jssc.start();
		// jssc.awaitTerminationOrTimeout(5000);
		jssc.awaitTermination();

	}
}
