package project2;
import java.io.*;
import java.util.*;
import java.util.Random;
/**
 * Created by Youqiao Ma on 2/19/2017.
 */
public class Problem1WriteFile {

    private static final int MAX_POINTS = 10000;
    private static final int MAX_RECTANGLES = 10000;

    private static final int MAX_AXIS = 10000;
    private static final int MIN_AXIS = 1;

    private static Random ra = new Random();

    public static void main(String[] args){
        new File("Output").mkdir();

        //generate Points file
        try {
            File file1 = new File("Output/Points");
            OutputStream out1 = new BufferedOutputStream(new FileOutputStream(file1));

            for(int i=0; i < MAX_POINTS; i++){
                out1.write(getPointLine(i+1).getBytes());
                if(i != MAX_POINTS-1){
                    out1.write('\n');
                }
            }

            out1.close();
            System.out.println("Points file is created.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //generate Transactions file
        try {
            File file2 = new File("Output/Rectangles");
            OutputStream out2 = new BufferedOutputStream(new FileOutputStream(file2));

            for(int i=0; i < MAX_RECTANGLES; i++){
                out2.write(getRectangleLine(i+1).getBytes());
                if(i != MAX_RECTANGLES-1){
                    out2.write('\n');
                }
            }

            out2.close();
            System.out.println("Rectangles file is created.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getPointLine(int seed){
        StringBuilder sb = new StringBuilder();
        sb.append(getRandomPoint(10000, MIN_AXIS, MAX_AXIS)).append(",");
        sb.append(getRandomPoint(10000, MIN_AXIS, MAX_AXIS));
        return "";
    }

    private static String getRandomPoint(int times, float lowbound, float highbound){
        float sample;
        do{
            sample = ra.nextFloat() * times;
        }while(sample < lowbound || sample > highbound);
        return String.format("%.2f", sample);
    }

    private static String getRectangleLine(int seed){
        return "";
    }

    private static String getRectanglePoint(int times, float lowbound, float highbound){
        float sample;
        do{
            sample = ra.nextFloat() * times;
        }while(sample < lowbound || sample > highbound);
        return String.format("%.2f", sample);
    }

    /*


    private Class Rectangle(){



        float vertex = 0;
        int width;
        int height;
    }
     */
}
