package edu.wisc.streaming;

import java.util.Random;

public class PerceptronDataGenerator {

    public static void main(String[] args)
    {
        if (args.length != 2)
            {
                System.err.println("Syntax: java PerceptronDataGenerator nameOfTestSetFile numOfIterations");
                System.exit(1);
            }

        // Read in the file names.
        String testset  = args[0];
        int numOfIterations = Integer.parseInt(args[1]);

        ListOfExamples testExamplesSet = new ListOfExamples();

        if (!testExamplesSet.ReadInExamplesFromFile(testset)) {
            System.err.println("Something went wrong reading the datasets ... " +
                               "giving up.");
            System.exit(1);
        }
        
        
        int size = testExamplesSet.size();
        
        for(int i=0; i<1; i++) {
        	ListOfExamples le = testExamplesSet.clone();
        	for(int j = 0; j<size; j++) {
            	Example e = le.remove(0);
            	e.printExample();
        	}
        }
        
        int count = 5000;
        
        //System.out.println("Boundary");
        Random rn = new Random();
        for(int i=0; i<numOfIterations; i++) {
        	ListOfExamples le = testExamplesSet.clone();
        	for(int j = size; j >0; j--) {
            	int randomNum =  rn.nextInt(j);
            	Example e = le.remove(randomNum);
            	e.setName("testEx"+count);
            	e.printExample();
            	count++;
        	}
        }
    }
}
