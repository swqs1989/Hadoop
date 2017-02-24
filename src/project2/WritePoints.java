package project2;

import util.Util;

import java.io.*;

/**
 * Created by Youqiao Ma on 2/22/2017.
 */
public class WritePoints {

    private static final int MAX_POINTS = 10000;
    private static final int MAX_RECTANGLES = 10000;

    private static final int MAX_AXIS = 10000;
    private static final int MIN_AXIS = 1;

    public static void main(String args[]){
        //generate Points file
        try {
            File file1 = new File("Output/Data");
            OutputStream out1 = new BufferedOutputStream(new FileOutputStream(file1));

            for(int i=0; i < MAX_POINTS; i++){
                out1.write(Util.getPointLine(MIN_AXIS, MAX_AXIS).getBytes());
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

        //generate Points file
        try {
            File file2 = new File("Output/InitPoints");
            OutputStream out2 = new BufferedOutputStream(new FileOutputStream(file2));

            for(int i=0; i < 1000; i++){
                out2.write(Util.getPointLine(MIN_AXIS, MAX_AXIS).getBytes());
                if(i != 1000-1){
                    out2.write('\n');
                }
            }

            out2.close();
            System.out.println("Points file is created.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
