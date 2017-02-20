package project2;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Youqiao Ma on 2/20/2017.
 */
public class Rectangle {

    public Rectangle(String input){
        StringTokenizer itr = new StringTokenizer(input, ",");
        name = itr.nextToken();
        x_axis = Float.parseFloat(itr.nextToken());
        y_axis = Float.parseFloat(itr.nextToken());
        width = Integer.parseInt(itr.nextToken());
        height = Integer.parseInt(itr.nextToken());
        area = new ArrayList<>();
        calArea();
    }

    private void calArea(){
        Float x = new Float(x_axis);
        Float y = new Float(y_axis);
        area.add(calArea(x, y));
        Float rx = new Float(x_axis + width);
        Float ry = new Float(y_axis - height);
        String righttop = calArea(rx, y);
        String leftbottom = calArea(x, ry);
        String rightbottom = calArea(rx, ry);
        if(!area.contains(righttop)){
            area.add(righttop);
        }
        if(!area.contains(leftbottom)){
            area.add(leftbottom);
        }
        if(!area.contains(rightbottom)){
            area.add(rightbottom);
        }
    }

    private String calArea(Float x, Float y){
        StringBuilder sb = new StringBuilder();
        if(x != 10000){
            x = x/1000;
            sb.append(x.intValue());
        }else{
            sb.append("9");
        }
        if(y != 10000){
            y = y/1000;
            sb.append(y.intValue());
        }else{
            sb.append("9");
        }
        return sb.toString();
    }

    public String toString(){
        return name + "," + x_axis + "," + y_axis + "," + width + "," + height;
    }

    public boolean isInside(Point p){
        float rx = x_axis + width;
        float ry = y_axis - height;
        if(p.getX_axis() <= rx && p.getX_axis() >= x_axis && p.getY_axis() <= y_axis && p.getY_axis() >= ry){
            return true;
        }else{
            return false;
        }
    }

    private String name;
    private float x_axis;
    private float y_axis;
    private int width;
    private int height;

    public ArrayList<String> area;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }
}
