package project1;

import java.io.*;
import java.util.*;
import java.util.Random;

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
                out1.write(getCustomerLine(i+1).getBytes());
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
                out2.write(getTransLine(i+1).getBytes());
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

    public static String getCustomerLine(int seed){
        StringBuilder sb = new StringBuilder();
        sb.append(seed).append(","); //ID: unique sequential number (integer) from 1 to 50,000
        sb.append(randomString(10,10)).append(","); //Name: random sequence of characters of length between 10 and 20
        sb.append(randomInt(10,70)).append(","); //Age: random number (integer) between 10 to 70
        sb.append(randomInt(1,10)).append(",");//CountryCode: random number (integer) between 1 and 10
        sb.append(String.format("%.2f", randomfloat(10000, 100,10000)));
        return sb.toString();
    }

    public static String getTransLine(int seed){
        StringBuilder sb = new StringBuilder();
        sb.append(seed).append(","); //TransID: unique sequential number from 1 to 5,000,000
        sb.append(getRandomCustomerID()).append(","); //CustomerID: References one of the customer IDs
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

    public static int getRandomCustomerID(){
        Random ra = new Random();
        return (ra.nextInt(customer_lines) + 1);
    }
}
