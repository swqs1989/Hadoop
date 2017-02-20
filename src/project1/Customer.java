package project1;

import java.util.StringTokenizer;

public class Customer{
	public Customer(String input){
		StringTokenizer itr = new StringTokenizer(input, ",");
		this.id = Integer.parseInt(itr.nextToken());
		this.name = itr.nextToken();
		this.age = Integer.parseInt(itr.nextToken());
		this.countrycode = Integer.parseInt(itr.nextToken());
		this.salary = Float.parseFloat(itr.nextToken());
	}

	public String getValueString(){
		return this.name + "," + this.age + "," + this.countrycode + "," + this.salary;
	}

	public int id;
	public String name;
	public int age;
	public int countrycode;
	public float salary;
}
