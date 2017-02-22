package project1;

import java.io.*;
import java.util.*;
import java.util.Random;
import util.*;

/**
 * Created by Youqiao Ma on 1/30/2017.
 */
public class WriteFile {

    private static String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
    public static List<String> ids = new ArrayList<String>();

    private static int customer_lines = 50000;
    private static int tran_lines = 5000000;

    public static void main(String[] args){

        new File("Output").mkdir();

        //generate Customers file
        try {
            File file1 = new File("Output/Customers");
            OutputStream out1 = new BufferedOutputStream(new FileOutputStream(file1));

            for(int i=0; i < customer_lines; i++){
                out1.write(Util.getCustomerLine(i+1).getBytes());
                if(i != customer_lines-1){
                    out1.write('\n');
                }
            }

            out1.close();
            System.out.println("Customers file is created.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //generate Transactions file
        try {
            File file2 = new File("Output/Transactions");
            OutputStream out2 = new BufferedOutputStream(new FileOutputStream(file2));

            for(int i=0; i < tran_lines; i++){
                out2.write(Util.getTransLine(i+1, customer_lines).getBytes());
                if(i != tran_lines-1){
                    out2.write('\n');
                }
            }

            out2.close();
            System.out.println("Transactions file is created.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
