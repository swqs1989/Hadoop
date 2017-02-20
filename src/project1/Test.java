package project1;

import java.io.*;
import java.util.HashMap;
import java.util.Random;
import java.util.StringTokenizer;
/**
 * Created by Youqiao Ma on 1/30/2017.
 */
public class Test {
    private static String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
    //private static HashMap<Integer, Integer> idCode = new HashMap<Integer, Integer>();
    public static void main(String[] args){

        Random ra = new Random();
        for (int i = 0; i < 20; i++) {
            System.out.println(ra.nextDouble());
        }


        /*


        for (int i = 0; i < 100; i++) {
            //float result = randomfloat(10000, 100, 10000);
            //if(result < 100 || result > 10000){
            String sample = randomString(5, 5);
            System.out.println(sample + " --- " + sample.length());
            //}

            //Random ra = new Random();
            //System.out.println(ra.nextInt(11));
        }
        */
    }

    public static int randomInt(int lowbound, int highbound){
        Random ra = new Random();
        return ra.nextInt(highbound - lowbound + 1)+lowbound;
    }

    public static float randomfloat(int times, float lowbound, float highbound){
        Random ra = new Random();
        float sample;
        do{
            sample = ra.nextFloat() * times;
        }while(sample < lowbound || sample > highbound);
        return sample;
    }

    public static String randomString(int span, int init){
        Random ra = new Random();
        StringBuilder sb = new StringBuilder();
        int length = ra.nextInt(span + 1) + init; //int length = ra.nextInt(10) + 11;
        for(int i = 0; i < length; i++){
            sb.append(characters.charAt(ra.nextInt(62)));
        }
        return sb.toString();
    }
}
