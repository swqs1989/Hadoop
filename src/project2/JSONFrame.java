package project2;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Youqiao Ma on 2/22/2017.
 */
public class JSONFrame {

    public JSONFrame(String value){

    }

    public static HashMap<String, String> parseJSON(String value){
        String value_new = value.replace("\t", "").replace("\n", "").trim();
        ArrayList<String> keys = new ArrayList<>();
        ArrayList<String> values = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean quote = false;
        boolean keyflag = true;
        for (char c: value_new.toCharArray()
             ) {
            if(keyflag){ //key phase
                if(c == '\"'){
                    quote = !quote;
                    if(quote){
                        continue;
                    }else{
                        keys.add(sb.toString());
                        sb = new StringBuilder();
                        keyflag = false;
                        continue;
                    }
                }
                if(quote){
                    sb.append(c);
                }
            }else{ //value phase
                if(c == ':' || c == '\"' || c == ' '){
                    continue;
                }
                if(c == ','){
                    values.add(sb.toString());
                    sb = new StringBuilder();
                    keyflag = true;
                    continue;
                }

                if(c == '}'){
                    values.add(sb.toString());
                    continue;
                }
                sb.append(c);
            }
        }

        HashMap<String, String> hm = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            hm.put(keys.get(i), values.get(i));
        }
        return hm;
    }

    public ArrayList<String> values;
}
