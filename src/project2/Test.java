package project2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.simple.parser.JSONParser;

/**
 * Created by Youqiao Ma on 2/20/2017.
 */
public class Test {
    public static void main(String[] args){
        String str1 = "           {\n" +
                "\t\t\"ID\": \"LFOI\",\n" +
                "\t\t\"ShortName\": \"ABBEV\",\n" +
                "\t\t\"Name\": \"ABBEVILLE\",\n" +
                "\t\t\"Region\": \"FR\",\n" +
                "\t\t\"ICAO\": \"LFOI\",\n" +
                "\t\t\"Flags\": 72,\n" +
                "\t\t\"Catalog\": 0,\n" +
                "\t\t\"Length\": 1260,\n" +
                "\t\t\"Elevation\": 67,\n" +
                "\t\t\"Runway\": \"0213\",\n" +
                "\t\t\"Frequency\": 0,\n" +
                "\t\t\"Latitude\": \"N500835\",\n" +
                "\t\t\"Longitude\": \"E0014954\"\n" +
                "\t}";
        HashMap<String, String> hm = JSONFrame.parseJSON(str1);

        hm.get("Flags");


        System.out.println(hm.get("Flags"));
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
    }
}
