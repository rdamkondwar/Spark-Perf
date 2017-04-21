package edu.wisc.streaming.util;

import java.io.Serializable;
import java.util.Random;

public class RandomWordGenerator implements Serializable {
    
    private static final long serialVersionUID = 1L;
    //private char [] dataSrc = new char[26];
    private Random r = new Random(System.currentTimeMillis());
    //private int MAX_LEN = 5;
    //private int MAX_IDX  = 5;
    private String[] words;
    
    public RandomWordGenerator() {
        // char start = 'a';
        // int i = 0;
        // while(i < 26) {
        //     dataSrc[i] = start;
        //     // System.out.printf("i=%d char=%c\n", i, dataSrc[i]);
        //     i++;
        //     start++;
        // }
	words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
    }

    public String generateString() {
        int index = r.nextInt(words.length);
        // System.out.println("debug1: Got ran_len = " + length);
        // char [] randomChars = new char[length];
        // for(int i=0; i < length; i++) {
        //     randomChars[i] = this.dataSrc[this.r.nextInt(MAX_IDX)];
        // }
        // System.out.println("debug1: Got ran_len = " + new String(this.dataSrc));
        return new String(words[index]);
    }

    public static void main(String[] args) {
        // RandomWordGenerator rwm = new RandomWordGenerator();
        // System.out.println(rwm.generateString());
        // System.out.println(rwm.generateString());
        // System.out.println(rwm.generateString());
        // System.out.println(rwm.generateString());
        // System.out.println(rwm.generateString());
    }
}
