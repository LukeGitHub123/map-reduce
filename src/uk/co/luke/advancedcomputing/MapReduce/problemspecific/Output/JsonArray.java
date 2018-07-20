package uk.co.luke.advancedcomputing.MapReduce.problemspecific.Output;

import java.util.ArrayList;

public class JsonArray {
	ArrayList<Object> elements;
	
	public JsonArray(){
		elements = new ArrayList<>();
	}
	
	public void addJsonObject( JsonObject jsonObject){
		elements.add(jsonObject);
	}
	
	public void addJsonArray( JsonArray array){
		elements.add(array);
	}
	
	public void addString(String value){
		elements.add(value);
	}
	
	public void addInteger(int number){
		elements.add(number);
	}
	
	public void addFloat(float value){
		elements.add(value);
	}
	
	@Override
	public String toString() {
		return JsonUtilities.print(this);
	}
}
