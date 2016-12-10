package edu.wisc.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
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

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class RunPerceptron {

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err
					.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
							+ "  <brokers> is a list of one or more Kafka brokers\n"
							+ "  <topics> is a list of one or more kafka topics to consume from\n <max_rate>\n <batch_duration_in_ms> \n");
			System.exit(1);
		}
		
		String brokers = args[0];
		String topics = args[1];
		String maxRate = ((Long)Long.parseLong(args[2])).toString();
		long batchDuration = Long.parseLong(args[3]);

		SparkConf sparkConf = new SparkConf()
		    .setAppName("RunPerceptron").set("spark.streaming.kafka.maxRatePerPartition", maxRate);
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.milliseconds(batchDuration));
		
		jssc.checkpoint("/tmp/services/checkpoint");

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		kafkaParams.put("auto.offset.reset", "smallest");
		

        Integer NUM_OF_FEATURES = 20;
        
        Double[] w_arr = new Double[] {-0.30,0.20,0.20,0.10,-0.00,-0.10,-0.30,0.20,0.40,-0.10,0.10,0.40,-0.10,-0.70,-0.20,-0.20,0.10,-0.50,-0.00,-0.90,-0.10};

        ListOfExamples testExamplesSet = new ListOfExamples();
        
        BinaryFeature[] features = new BinaryFeature[20];
        features[0] = new BinaryFeature("FixedAcidityGt47","T","F");
        features[1] = new BinaryFeature("volatileAcidityGt17","T","F");
	    features[2] = new BinaryFeature("volatileAcidityGt29","T","F");
	    features[3] = new BinaryFeature("citricAcidGt30","T","F");
		features[4] = new BinaryFeature("residualSugarGtMean","T","F");
		features[5] = new BinaryFeature("chloridesGt9","T","F");
		features[6] = new BinaryFeature("freeSulfurDioxideGtMean","T","F");
		features[7] = new BinaryFeature("totalSulfurDioxideGt27","T","F");
		features[8] = new BinaryFeature("totalSulfurDioxideGt37","T","F");
		features[9] = new BinaryFeature("totalSulfurDioxideGt54","T","F");
		features[10] = new BinaryFeature("densityGt18","T","F");
		features[11] = new BinaryFeature("densityGt41","T","F");
		features[12] = new BinaryFeature("pHGtMean","T","F");
		features[13] = new BinaryFeature("sulphatesGt12","T","F");
		features[14] = new BinaryFeature("sulphatesGt15","T","F");
		features[15] = new BinaryFeature("sulphatesGt19","T","F");
		features[16] = new BinaryFeature("sulphatesGt44","T","F");
		features[17] = new BinaryFeature("alcoholGt22","T","F");
		features[18] = new BinaryFeature("alcoholGt33","T","F");
		features[19] = new BinaryFeature("alcoholGt47","T","F");
		
		testExamplesSet.setFeatures(NUM_OF_FEATURES, features);

        Perceptron perceptron = new Perceptron(w_arr, NUM_OF_FEATURES);
		
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);
		
		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = -8970957373064338596L;

					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		JavaPairDStream<Example, Integer> predictions = lines.mapToPair(
				new PairFunction<String, Example, Integer>() {
					
					private static final long serialVersionUID = 4570173482139737976L;

					@Override
					public Tuple2<Example, Integer> call(String s) {
						Example e = ListOfExamples.parseExampleFromString(s, testExamplesSet);
						int output = perceptron.runExample(e);
						return new Tuple2<>(e, output);
					}
		});
			
//		Function3<Example, Optional<Integer>, State<Integer>, Tuple2<Example, Integer>> mappingFunc = new Function3<Example, Optional<Integer>, State<Integer>, Tuple2<Example, Integer>>() {
//			private static final long serialVersionUID = -4724201592652717034L;
//
//			@Override
//			public Tuple2<Example, Integer> call(Example word, Optional<Integer> one, State<Integer> state) {
//				//int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
//				Tuple2<Example, Integer> output = new Tuple2<>(word, one.get());
//				state.update(one.get());
//				return output;
//			}
//		};
//
//		JavaMapWithStateDStream<Example, Integer, Integer, Tuple2<Example, Integer>> stateDstream =
//		        predictions.mapWithState(StateSpec.function(mappingFunc));
//		
//		
//		stateDstream.stateSnapshots().print();
		predictions.count().print();
		
		// stateDstream.repartition(1);
		//stateDstream.stateSnapshots().repartition(1).dstream().saveAsTextFiles("/tmp/services/output", "");
		
		// Start the computation
		jssc.start();
		// jssc.awaitTerminationOrTimeout(5000);
		jssc.awaitTermination();
	}
}
