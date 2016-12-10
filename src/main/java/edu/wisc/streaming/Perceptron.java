package edu.wisc.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

class Stats implements Serializable {
	private static final long serialVersionUID = -3612104666126306634L;
	private int correct = 0, wrong = 0, total = 0;
    private double accuracy; 
    
    public Stats(int c, int w) {
        this.correct = c;
        this.wrong = w;
        this.total = this.correct + this.wrong;
        this.accuracy = (this.correct * 100.0)/this.total;
    }
    
    public double getAccuracy() {
        return this.accuracy;
    }
}

public class Perceptron implements Serializable {
	
	private static final long serialVersionUID = 5146134741283791340L;
	private int numOfInputs;
    private List<Double> weights;
    private int bais_input = -1;
    private Double threshold_wt = 0.0d;
    private double alpha = 0.1d;
    
    public Perceptron(Double[] weights_arr, int numOfInputs) {
    	//Remove 1 for bias input of -1
    	this.numOfInputs = numOfInputs;
    	weights = new ArrayList<Double>(this.numOfInputs);
    	
        for (int i = 0; i < numOfInputs; i++) {
            this.weights.add(weights_arr[i]);
        }
        this.threshold_wt = weights_arr[numOfInputs];
    }

    public Perceptron(ListOfExamples examples) {
        numOfInputs = examples.features.length;
        // System.out.println("numOfinputs = " + numOfInputs);
        weights = new ArrayList<>(numOfInputs);
        for (int i=0; i < numOfInputs; i++) {
            weights.add(0.0d);
        }
    }

    public void printWeights(ListOfExamples examples) {
        int i = 0;
        for (double w: weights) {
            //System.out.printf("w=%.2f %s\n", w, examples.getFeatureName(i));
        	System.out.printf("%.2f\n", w);
            i++;
        }
        System.out.printf("%.2f\n", threshold_wt);
    }

    private double updateWeight(double w, int actual, int calculated, int input) {
        return w + alpha*((double)(actual - calculated))*((double)input);
    }

    public void updateWeights(Example e, int actual, int calculated) {
        for(int i=0; i < e.size(); i++) {
            double w = weights.get(i);
            double new_w = this.updateWeight(w, actual, calculated, e.get(i));
            weights.set(i, new_w);
        }
        this.threshold_wt = this.updateWeight(this.threshold_wt, actual, calculated, this.bais_input);
    }

    public Integer runExample(Example e) {
        double output = 0.0d;
        for (int i = 0; i < weights.size(); i++) {
            // System.out.println("Feature val = " + e.get(i) + " w=" + weights.get(i));
            output += (double)e.get(i) * weights.get(i);
        }
        output += (double)bais_input * threshold_wt;
        if (output >= 0.0d) {
            return 1;
        }
        return 0;
    }

    public Stats runExamplesFromIndex(ListOfExamples examples, int start_idx, boolean updateWeights) {
        int correct = 0, wrong = 0, total = 0;
        for (int i = 0; i < examples.size(); i++) {
            Example e = examples.get((i + start_idx) % examples.size());
            int calculated = this.runExample(e);
            int actual = examples.outputLabel.getFeatureValueIndex(e.getLabel());
            // System.out.printf("%d %d %s\n", actual, calculated, e.getLabel());
            if (actual != calculated) {
                
                // e.PrintFeatures();
                if (updateWeights) {
                    this.updateWeights(e, actual, calculated);
                }
                wrong++;
            }
            else {
                correct++;
            }
            total++;
        }
        // System.out.printf("Total = %d correct = %d wrong = %d\n", total, correct, wrong);
        return new Stats(correct, wrong);
    }

    public void runAllExamples(ListOfExamples examples) {
        int correct = 0, wrong = 0, total = 0;
        for (Example e : examples) {
            int calculated = this.runExample(e);
            int actual = examples.outputLabel.getFeatureValueIndex(e.getLabel());
            // System.out.printf("%d %d\n", calculated, actual);
            if (actual != calculated) {
                // System.out.println("Example" + e.getName());
                // e.PrintFeatures();
                this.updateWeights(e, actual, calculated);
                wrong++;
            }
            else {
                correct++;
            }
            total++;
        }
        // System.out.printf("Total = %d correct = %d wrong = %d\n", total, correct, wrong);
    }
}

// This class, an extension of ArrayList, holds an individual example.
// The new method PrintFeatures() can be used to
// display the contents of the example.
// The items in the ArrayList are the feature values.
class Example extends ArrayList<Integer> implements Cloneable, Serializable
{
	private static final long serialVersionUID = 8368342836702700119L;

	// The name of this example.
    private String name;

    // The output label of this example.
    private String label;

    // The data set in which this is one example.
    private ListOfExamples parent;

    // Constructor which stores the dataset which the example belongs to.
    public Example(ListOfExamples parent) {
        this.parent = parent;
    }
    
    public void printExample() {
    	System.out.print(name + " " + label);
        for (int i = 0; i < parent.getNumberOfFeatures(); i++)
            {
                System.out.print(" " + this.get(i));
            }
        System.out.print("\n");
    }

    // Print out this example in human-readable form.
    public void PrintFeatures()
    {
        System.out.print("Example " + name + ",  label = " + label + "\n");
        for (int i = 0; i < parent.getNumberOfFeatures(); i++)
            {
                System.out.print("     " + parent.getFeatureName(i)
                                 + " = " +  this.get(i) + "\n");
            }
    }

    // Adds a feature value to the example.
    public void addFeatureValue(Integer value) {
        this.add(value);
    }

    // Accessor methods.
    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    // Mutator methods.
    public void setName(String name) {
        this.name = name;
    }

    public void setLabel(String label) {
        this.label = label;
    }
    
    public Example clone() {
    	Example e = new Example(this.parent);
    	e.label = this.label;
    	e.name = this.name;
    	for(Integer v : this) {
    		e.add(v);
    	}
    	return e;
    }
    
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append(name).append(" ").append(label);
        for (int i = 0; i < parent.getNumberOfFeatures(); i++) {
        	sb.append(" ").append(this.get(i));
    	}
        return sb.toString();
    }
}
/* This class holds all of our examples from one dataset
   (train OR test, not BOTH).  It extends the ArrayList class.
   Be sure you're not confused.  We're using TWO types of ArrayLists.
   An Example is an ArrayList of feature values, while a ListOfExamples is
   an ArrayList of examples. Also, there is one ListOfExamples for the
   TRAINING SET and one for the TESTING SET.
*/
class ListOfExamples extends ArrayList<Example> implements Cloneable, Serializable
{
	private static final long serialVersionUID = -7863298034390017257L;

	// The name of the dataset.
    private String nameOfDataset = "";

    // The number of features per example in the dataset.
    private int numFeatures = -1;

    // An array of the parsed features in the data.
    public BinaryFeature[] features;

    // A binary feature representing the output label of the dataset.
    public BinaryFeature outputLabel;

    // The number of examples in the dataset.
    private int numExamples = -1;
    
    public ListOfExamples clone() {
    	ListOfExamples l = new ListOfExamples();
    	l.numFeatures = this.numFeatures;
    	l.features = this.features;
    	l.outputLabel = this.outputLabel;
    	for(Example e : this) {
    		l.add(e.clone());
    	}
    	return l;
    }

    public ListOfExamples() {}
    
    public void setFeatures(int numFeatures, BinaryFeature[] features) {
    	this.numFeatures = numFeatures;
    	this.features = features;
    }

    // Print out a high-level description of the dataset including its features.
    public void DescribeDataset()
    {
        System.out.println("Dataset '" + nameOfDataset + "' contains "
                           + numExamples + " examples, each with "
                           + numFeatures + " features.");
        System.out.println("Valid category labels: "
                           + outputLabel.getFirstValue() + ", "
                           + outputLabel.getSecondValue());
        System.out.println("The feature names (with their possible values) are:");
        for (int i = 0; i < numFeatures; i++)
            {
                BinaryFeature f = features[i];
                System.out.println("   " + f.getName() + " (" + f.getFirstValue() +
                                   " or " + f.getSecondValue() + ")");
            }
        System.out.println();
    }

    // Print out ALL the examples.
    public void PrintAllExamples()
    {
        System.out.println("List of Examples\n================");
        for (int i = 0; i < size(); i++)
            {
                Example thisExample = this.get(i);
                thisExample.PrintFeatures();
            }
    }

    // Print out the SPECIFIED example.
    public void PrintThisExample(int i)
    {
        Example thisExample = this.get(i);
        thisExample.PrintFeatures();
    }

    // Returns the number of features in the data.
    public int getNumberOfFeatures() {
        return numFeatures;
    }

    // Returns the name of the ith feature.
    public String getFeatureName(int i) {
        return features[i].getName();
    }
    
    // Takes the name of an input file and attempts to open it for parsing.
    // If it is successful, it reads the dataset into its internal structures.
    // Returns true if the read was successful.
    public boolean ReadInExamplesFromFile(String dataFile) {
        nameOfDataset = dataFile;

        // Try creating a scanner to read the input file.
        Scanner fileScanner = null;
        try {
            fileScanner = new Scanner(new File(dataFile));
        } catch(FileNotFoundException e) {
            return false;
        }

        // If the file was successfully opened, read the file
        this.parse(fileScanner);
        return true;
    }

    /**
     * Does the actual parsing work. We assume that the file is in proper format.
     *
     * @param fileScanner a Scanner which has been successfully opened to read
     * the dataset file
     */
    public void parse(Scanner fileScanner) {
        // Read the number of features per example.
        numFeatures = Integer.parseInt(parseSingleToken(fileScanner));

        // Parse the features from the file.
        parseFeatures(fileScanner);

        // Read the two possible output label values.
        String labelName = "output";
        String firstValue = parseSingleToken(fileScanner);
        String secondValue = parseSingleToken(fileScanner);
        outputLabel = new BinaryFeature(labelName, firstValue, secondValue);

        // Read the number of examples from the file.
        numExamples = Integer.parseInt(parseSingleToken(fileScanner));

        parseExamples(fileScanner);
    }

    /**
     * Returns the first token encountered on a significant line in the file.
     *
     * @param fileScanner a Scanner used to read the file.
     */
    private String parseSingleToken(Scanner fileScanner) {
        String line = findSignificantLine(fileScanner);

        // Once we find a significant line, parse the first token on the
        // line and return it.
        Scanner lineScanner = new Scanner(line);
        return lineScanner.next();
    }

    /**
     * Reads in the feature metadata from the file.
     *
     * @param fileScanner a Scanner used to read the file.
     */
    private void parseFeatures(Scanner fileScanner) {
        // Initialize the array of features to fill.
        features = new BinaryFeature[numFeatures];

        for(int i = 0; i < numFeatures; i++) {
            String line = findSignificantLine(fileScanner);

            // Once we find a significant line, read the feature description
            // from it.
            Scanner lineScanner = new Scanner(line);
            String name = lineScanner.next();
            String dash = lineScanner.next();  // Skip the dash in the file.
            String firstValue = lineScanner.next();
            String secondValue = lineScanner.next();
            features[i] = new BinaryFeature(name, firstValue, secondValue);
        }
    }
    
    public static Example parseExampleFromString(String line, ListOfExamples parent) {
    	//String line = findSignificantLine(fileScanner);
        Scanner lineScanner = new Scanner(line);

        // Parse a new example from the file.
        Example ex = new Example(parent);

        String name = lineScanner.next();
        ex.setName(name);

        String label = lineScanner.next();
        ex.setLabel(label);

        // Iterate through the features and increment the count for any feature
        // that has the first possible value.
        for(int j = 0; j < parent.numFeatures; j++) {
            String feature = lineScanner.next();
            int value = parent.features[j].getFeatureValueIndex(feature);
            ex.addFeatureValue(value);
        }

        return ex;
    }
    
    private void parseExamples(Scanner fileScanner) {
        // Parse the expected number of examples.
        for(int i = 0; i < numExamples; i++) {
            String line = findSignificantLine(fileScanner);
            Scanner lineScanner = new Scanner(line);

            // Parse a new example from the file.
            Example ex = new Example(this);

            String name = lineScanner.next();
            ex.setName(name);

            String label = lineScanner.next();
            ex.setLabel(label);

            // Iterate through the features and increment the count for any feature
            // that has the first possible value.
            for(int j = 0; j < numFeatures; j++) {
                String feature = lineScanner.next();
                int value = features[j].getFeatureValueIndex(feature);
                ex.addFeatureValue(value);
            }

            // Add this example to the list.
            this.add(ex);
        }
    }

    /**
     * Returns the next line in the file which is significant (i.e. is not
     * all whitespace or a comment.
     *
     * @param fileScanner a Scanner used to read the file
     */
    private String findSignificantLine(Scanner fileScanner) {
        // Keep scanning lines until we find a significant one.
        while(fileScanner.hasNextLine()) {
            String line = fileScanner.nextLine().trim();
            if (isLineSignificant(line)) {
                return line;
            }
        }

        // If the file is in proper format, this should never happen.
        System.err.println("Unexpected problem in findSignificantLine.");

        return null;
    }

    /**
     * Returns whether the given line is significant (i.e., not blank or a
     * comment). The line should be trimmed before calling this.
     *
     * @param line the line to check
     */
    private boolean isLineSignificant(String line) {
        // Blank lines are not significant.
        if(line.length() == 0) {
            return false;
        }

        // Lines which have consecutive forward slashes as their first two
        // characters are comments and are not significant.
        if(line.length() > 2 && line.substring(0,2).equals("//")) {
            return false;
        }

        return true;
    }
}

/**
 * Represents a single binary feature with two String values.
 */
class BinaryFeature implements Serializable {
	private static final long serialVersionUID = -7380255622825720728L;
	private String name;
    private String firstValue;
    private String secondValue;

    public BinaryFeature(String name, String first, String second) {
        this.name = name;
        firstValue = first;
        secondValue = second;
    }

    public String getName() {
        return name;
    }

    public String getFirstValue() {
        return firstValue;
    }

    public String getSecondValue() {
        return secondValue;
    }

    public int getFeatureValueIndex(String value) {
        if(this.firstValue.equals(value)) {
            return 0;
        }
        if(this.secondValue.equals(value)) {
            return 1;
        }
        return -1;
    }
}

//class Utilities 
//{
//    // This method can be used to wait until you're ready to proceed.
//    public static void waitHere(String msg)
//    {
//        System.out.print("\n" + msg);
//        try { System.in.read(); }
//        catch(Exception e) {} // Ignore any errors while reading.
//    }
//}
