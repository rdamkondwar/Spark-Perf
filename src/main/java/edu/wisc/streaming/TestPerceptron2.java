package edu.wisc.streaming;

public class TestPerceptron2 {

    public static void main(String[] args)
    {
        if (args.length != 1)
            {
                System.err.println("Syntax: java TestPerceptron2 <example_string>");
                System.exit(1);
            }
        
        // Read in the file names.
        String example_str = args[0];
        
        Integer NUM_OF_FEATURES = 20;
        
        Double[] w_arr = new Double[] {-0.30,0.20,0.20,0.10,-0.00,-0.10,-0.30,0.20,0.40,-0.10,0.10,0.40,-0.10,-0.70,-0.20,-0.20,0.10,-0.50,-0.00,-0.90,-0.10};

        // Read in the examples from the files.
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

        Perceptron p1 = new Perceptron(w_arr, NUM_OF_FEATURES);
        p1.printWeights(testExamplesSet);
        Example e = ListOfExamples.parseExampleFromString(example_str, testExamplesSet);
        
        p1.runExample(e);

        // trainExamplesSet.get(0).PrintFeatures();
        
    }
}
