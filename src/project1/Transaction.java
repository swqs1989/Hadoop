package project1;

import java.util.StringTokenizer;

/**
 * Created by Youqiao Ma on 2/5/2017.
 */
public class Transaction {
    public Transaction(String input){
        this.value = input;
        StringTokenizer itr = new StringTokenizer(input, ",");
        this.transID = Integer.parseInt(itr.nextToken());
        this.custID = Integer.parseInt(itr.nextToken());
        this.transTotal = Float.parseFloat(itr.nextToken());
        this.transNumItems = Integer.parseInt(itr.nextToken());
        this.transDesc = itr.nextToken();
    }

    public String value;
    public int transID;
    public int custID;
    public float transTotal;
    public int transNumItems;
    public String transDesc;
}
