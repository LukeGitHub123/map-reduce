package uk.co.luke.advancedcomputing.MapReduce.problemspecific.Output;

import java.util.HashMap;

public class JsonObject {
	
	HashMap<String, Object> attributes;
	
	public JsonObject(){
		attributes = new HashMap<>();
	}
	
	public void addJsonObject( String key, JsonObject jsonObject){
		attributes.put(key,jsonObject);
	}
	
	public void addJsonArray( String key ,JsonArray array){
		attributes.put(key, array);
	}
	
	public void addString(String key, String value){
		attributes.put(key, value);
	}
	
	public void addInteger(String key,int number){
		attributes.put(key,number);
	}
	
	public void addFloat(String key, float value){
		attributes.put(key, value);
	}
	
	
	@Override
	public String toString() {
		return JsonUtilities.print(this);
	}
}
