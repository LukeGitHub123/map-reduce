package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

import java.util.ArrayList;

/**
 * The InputException class is designed to be able to store extra information about invalid input.
 */
public class InputException extends Exception {
	
	String inputName;
	ArrayList<String> reasons;
	public InputException(String inputName, ArrayList<String> reasons){
		super(inputName + ": " + reasons + "\n\n");
		
		setInputName(inputName);
		setReasons( reasons);
	}
	
	public String getInputName() {
		return inputName;
	}
	
	public void setInputName(String inputName) {
		this.inputName = inputName;
	}
	
	public ArrayList<String> getReasons() {
		return reasons;
	}
	
	public void setReasons(ArrayList<String> reasons) {
		this.reasons = reasons;
	}
	
	public String getErrorAsJsonString(){
		String jsonString = "\t{\n" +
									"\t\tInputName: '" +  getInputName() + "',\n" +
									"\t\tErrors: [";
		boolean isFirstElement = true;
		for (String error : getReasons()){
			// if it is not the first element then add a comma as their is an element before it
			if(!isFirstElement){
				jsonString += ',';
			}else{
				isFirstElement = false;
			}
			// add the error
			jsonString  += '\'' + error + '\'';
		}
		jsonString += "]\n\t}";
		return jsonString;
	}
}
