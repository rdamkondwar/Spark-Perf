package edu.wisc.streaming;

public class TestPerceptron {

    public static void main(String[] args)
    {
        if (args.length != 3)
            {
                System.err.println("Syntax: java HW4 nameOfTrainSetFile nameOfTuneSetFile nameOfTestSetFile");
                System.exit(1);
            }

        // Read in the file names.
        String trainset = args[0];
        String tuneset = args[1];
        String testset  = args[2];

        // Read in the examples from the files.
        ListOfExamples trainExamplesSet = new ListOfExamples();

        if (!trainExamplesSet.ReadInExamplesFromFile(trainset)) {
            System.err.println("Something went wrong reading the datasets ... " +
                               "giving up.");
            System.exit(1);
        }

        ListOfExamples tuneExamplesSet = new ListOfExamples();

        if (!tuneExamplesSet.ReadInExamplesFromFile(tuneset)) {
            System.err.println("Something went wrong reading the datasets ... " +
                               "giving up.");
            System.exit(1);
        }
        
        ListOfExamples testExamplesSet = new ListOfExamples();

        if (!testExamplesSet.ReadInExamplesFromFile(testset)) {
            System.err.println("Something went wrong reading the datasets ... " +
                               "giving up.");
            System.exit(1);
        }

        // trainExamplesSet.PrintAllExamples();
        // Perceptron p = new Perceptron(trainExamplesSet);
        // System.out.println(p.runExample(trainExamplesSet.get(0)));
        // p.runAllExamples(trainExamplesSet);
        // p.printWeights();

        Perceptron p1 = new Perceptron(trainExamplesSet);
        
        for(int i=0; i < 1000; i++) {
            // System.out.println("run");
            p1.runExamplesFromIndex(trainExamplesSet, i, true);
            if ((i+1) % 50 == 0) {
                // Stats s_train = p1.runExamplesFromIndex(trainExamplesSet, i, true);
                Stats s_train = p1.runExamplesFromIndex(trainExamplesSet, i, false);
                Stats s_tune = p1.runExamplesFromIndex(tuneExamplesSet, 0, false);
                Stats s_test = p1.runExamplesFromIndex(testExamplesSet, 0, false);

                System.out.printf("Epoch %d: train = %.2f tune = %.2f test = %.2f\n", i+1, s_train.getAccuracy(), s_tune.getAccuracy(), s_test.getAccuracy());
                // System.out.printf("%d \t %.2f \t %.2f \t %.2f\n", i+1, s_train.getAccuracy(), s_tune.getAccuracy(), s_test.getAccuracy());
            }
        }

        // trainExamplesSet.get(0).PrintFeatures();
        p1.printWeights(trainExamplesSet);
    }
}
