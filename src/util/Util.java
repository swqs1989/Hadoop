package util;

import java.util.Random;

/**
 * Created by Youqiao Ma on 2/20/2017.
 */
public class Util {
    private static Random ra = new Random();
    private static final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";


    public static String getCustomerLine(int seed){
        StringBuilder sb = new StringBuilder();
        sb.append(seed).append(","); //ID: unique sequential number (integer) from 1 to 50,000
        sb.append(randomString(10,10)).append(","); //Name: random sequence of characters of length between 10 and 20
        sb.append(randomInt(10,70)).append(","); //Age: random number (integer) between 10 to 70
        sb.append(randomInt(1,10)).append(",");//CountryCode: random number (integer) between 1 and 10
        sb.append(String.format("%.2f", randomfloat(10000, 100,10000)));
        return sb.toString();
    }

    public static String getTransLine(int seed, int numCustomers){
        StringBuilder sb = new StringBuilder();
        sb.append(seed).append(","); //TransID: unique sequential number from 1 to 5,000,000
        sb.append(getRandomCustomerID(numCustomers)).append(","); //CustomerID: References one of the customer IDs
        sb.append(String.format("%.2f", randomfloat(1000, 10,1000))).append(","); //TransTotal: random number (float) between 10 and 1000
        sb.append(randomInt(1,10)).append(",");//TransNumItems: random number between 1 and 10
        sb.append(randomString(30,20));
        return sb.toString();
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

    public static int getRandomCustomerID(int num){
        Random ra = new Random();
        return (ra.nextInt(num) + 1);
    }

    public static String getPointLine(int min_axis, int max_axis){
        StringBuilder sb = new StringBuilder();
        sb.append(getRandomPoint(10000, min_axis, max_axis)).append(",");
        sb.append(getRandomPoint(10000, min_axis, max_axis));
        return sb.toString();
    }

    public static String getRandomPoint(int times, float lowbound, float highbound){
        float sample;
        do{
            sample = ra.nextFloat() * times;
        }while(sample < lowbound || sample > highbound);
        return String.format("%.2f", sample);
    }

    public static String getRectangleLine(int seed, int times, float lowbound, float highbound){
        return "r" + seed + "," + getRectanglePoint(times, lowbound, highbound);
    }

    public static String getRectanglePoint(int times, float lowbound, float highbound){
        float vertex_x;
        float vertex_y;
        int width = 0;
        int height = 0;
        do{
            vertex_x = Float.parseFloat(getRandomPoint(times, lowbound, highbound));
            vertex_y = Float.parseFloat(getRandomPoint(times, lowbound, highbound));
            width = randomInt(1,5);
            height = randomInt(1,20);
        }while(vertex_x < lowbound || vertex_x > highbound || (vertex_x + width) > highbound || (vertex_y - height) < lowbound);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%.2f", vertex_x)).append(",");
        sb.append(String.format("%.2f", vertex_y)).append(",");
        sb.append(width).append(",");
        sb.append(height);
        return sb.toString();
    }
}
