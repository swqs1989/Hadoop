package project2;

import java.util.StringTokenizer;

/**
 * Created by Youqiao Ma on 2/20/2017.
 */
public class Point {

    public Point(String input){
        StringTokenizer itr = new StringTokenizer(input, ",");
        x_axis = Float.parseFloat(itr.nextToken());
        y_axis = Float.parseFloat(itr.nextToken());
        calLocation(x_axis, y_axis);
    }

    private void calLocation(float x, float y){
        Float xn = new Float(x);
        Float yn = new Float(y);
        StringBuilder sb = new StringBuilder();
        if(xn != 10000){
            xn = xn/1000;
            sb.append(xn.intValue());
        }else{
            sb.append("9");
        }
        if(yn != 10000){
            yn = yn/1000;
            sb.append(yn.intValue());
        }else{
            sb.append("9");
        }
        location = sb.toString();
    }

    public float getX_axis() {
        return x_axis;
    }

    public void setX_axis(float x_axis) {
        this.x_axis = x_axis;
    }

    public float getY_axis() {
        return y_axis;
    }

    public void setY_axis(float y_axis) {
        this.y_axis = y_axis;
    }

    public String location;

    public String toString(){
        return x_axis + "," + y_axis;
    }

    private float x_axis;
    private float y_axis;
}
