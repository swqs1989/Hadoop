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
        int iloop = 100;
        int jloop = 10;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 20; j++) {
                if(jloop >= 15)
                    break;
                jloop++;
            }
            iloop--;
        }
        System.out.println(iloop);
        System.out.println(jloop);
        System.out.println();
        System.out.println();
        System.out.println();
    }
}
