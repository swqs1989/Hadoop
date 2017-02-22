package project1;

import java.io.*;
import java.util.ArrayList;
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
        ArrayList<String> area = new ArrayList<>();
        String a = "123";
        String b = "123";
        area.add(a);
        if(area.contains(b)){
            System.out.println("Value reference");
        }
        /*


        for (int i = 0; i < 100; i++) {
            float a = Float.parseFloat(getRandomPoint(10000,1,10000));
            float b = Float.parseFloat(getRandomPoint(10000,1,10000));
            System.out.println(a + ", " + b + ", " + getArea(a,b));
        }
         */
        /*
        Random ra = new Random();
        float min = 1000;
        float max = 0;
        for (int i = 0; i < 50000; i++) {
            String result = getRandomPoint(10000,1,10000);
            float f1 = Float.parseFloat(result);
            if(f1 > max){
                max = f1;
            }
            if(f1 < min){
                min = f1;
            }
            //System.out.println(f1);
        }

        System.out.println(min);
        System.out.println(max);
        */

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

    public static String getArea(float x_axis, float y_axis){
        Float x = new Float(x_axis);
        Float y = new Float(y_axis);
        StringBuilder sb = new StringBuilder();
        if(x != 10000){
            x = x/1000;
            sb.append(x.intValue());
        }else{
            sb.append("9");
        }
        if(y != 10000){
            y = y/1000;
            sb.append(y.intValue());
        }else{
            sb.append("9");
        }
        return sb.toString();
    }


    public static int getLocation(float a){
        a = a/1000;
        if(a != 10000){
            Double d = new Double(a);
            return d.intValue();
        }else{
            return 9;
        }
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

    private static String getRandomPoint(int times, float lowbound, float highbound){
        Random ra = new Random();
        float sample;
        do{
            sample = ra.nextFloat() * times;
        }while(sample < lowbound || sample > highbound);
        return String.format("%.2f", sample);
    }
}
