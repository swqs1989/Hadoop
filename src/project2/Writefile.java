package project2;

/**
 * Created by Youqiao Ma on 2/22/2017.
 */

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;

public class Writefile{
    public static void main(String[] args){
        // The name of the file to open.
        String inputfileName = "airfield.txt";

        // This will reference one line at a time
        String line = null;

        LinkedList<String> lls = new LinkedList<>();

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(inputfileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                lls.add(line);
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            inputfileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + inputfileName + "'");
        }

        //generate Points file
        try {
            File file1 = new File("new");
            OutputStream out1 = new BufferedOutputStream(new FileOutputStream(file1));

            for(int i=0; i < 1000; i++){
                Iterator<String> itr = lls.iterator();
                while(itr.hasNext()){
                    out1.write(itr.next().getBytes());
                }

            }

            out1.close();
            System.out.println("new file is created.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}