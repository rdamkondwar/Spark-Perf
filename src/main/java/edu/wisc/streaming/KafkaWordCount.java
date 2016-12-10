package edu.wisc.streaming;

import java.lang.Long;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.esotericsoftware.minlog.Log;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 * 
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public class KafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.err
					.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
							+ "  <brokers> is a list of one or more Kafka brokers\n"
							+ "  <topics> is a list of one or more kafka topics to consume from\n <max_rate>\n <batch_duration_in_ms> \n");
			System.exit(1);
		}
		
		// StreamingExamples.setStreamingLogLevels();

		String brokers = args[0];
		String topics = args[1];
		String maxRate = ((Long)Long.parseLong(args[2])).toString();
		long batchDuration = Long.parseLong(args[3]);
		Integer partitions = Integer.parseInt(args[4]);

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf()
		    .setAppName("JavaDirectKafkaWordCount").set("spark.streaming.kafka.maxRatePerPartition", maxRate);
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.milliseconds(batchDuration));
		
		jssc.checkpoint("/tmp/services/checkpoint");

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		kafkaParams.put("auto.offset.reset", "smallest");
		
		//OffsetRange r1 = new OffsetRange(topics, 1, 0, 1000000);
		//Set<OffsetRange> offsetRangeSet = new HashSet<OffsetRange>();
		//offsetRangeSet.add(r1);
//		Map<TopicAndPartition,Long> fromOffsets = new HashMap<>();
//		fromOffsets.put(new TopicAndPartition(topics, 1), )
//		
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);
		
		if (null != partitions) {
			messages.repartition(partitions);
		}
		//messages.window(Durations.milliseconds(100), Durations.milliseconds(300));

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		Log.error("Start Current Time: " + System.currentTimeMillis());

		// lines.count().print();

		// System.out.println("Hello World !!!!\n");
		// lines.print();

		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
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
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		Log.error("End Current Time: " + System.currentTimeMillis());
		//wordCounts.print();

		Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc = new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) {
				int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
				Tuple2<String, Integer> output = new Tuple2<>(word, sum);
				state.update(sum);
				return output;
			}
		};

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
		        wordCounts.mapWithState(StateSpec.function(mappingFunc));
		
		
		//stateDstream.stateSnapshots().print();
		stateDstream.count().print();
		// stateDstream.repartition(1);
		//stateDstream.stateSnapshots().repartition(1).dstream().saveAsTextFiles("/tmp/services/output", "");
		
		//stateDstream.

		// JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String,
		// Integer>> stateDStream = wordCounts.mapWithState(StateSpec<String,
		// Integer, StateType, MappedType>.function(updateFunction))

		
		// wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>()
		// {
		// @Override
		// public void call(JavaPairRDD<String, Integer> arg0) throws Exception
		// {
		// if(arg0.count() == 0) {
		// System.out.println("Terminating as kafka does not have any new value.");
		// //wordCounts.print();
		// //jssc.stop();
		// }
		//
		// }
		// });
		// Start the computation
		jssc.start();
		// jssc.awaitTerminationOrTimeout(5000);
		jssc.awaitTermination();
	}
}
