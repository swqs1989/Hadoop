package project2;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by Youqiao Ma on 2/20/2017.
 */
public class Test {
    public static void main(String[] args){
        LinkedList<String> pll = new LinkedList();
        pll.add("1,2,3");
        pll.add("1,2,3");
        pll.add("1,2,3");
        pll.add("1,2,3");
        pll.add("1,2,3");
        pll.add("edc,edc,edc");
        pll.add("abc,abc,abc");
        pll.add("abc,abc,abc");
        pll.add("abc,abc,abc");
        pll.add("abc,abc,abc");

        String[] temp = null;
        Iterator<String> itr = pll.iterator();
        while(itr.hasNext()){
            try{
                temp = itr.next().split(",");
                int i = Integer.parseInt(temp[0]);
            }catch (Exception e){
                String a = temp[0];
                System.out.println(a);
                break;
            }
        }

        while(itr.hasNext()){

            temp = itr.next().split(",");
            String a = temp[0];
            System.out.println(a);
        }
    }
}
