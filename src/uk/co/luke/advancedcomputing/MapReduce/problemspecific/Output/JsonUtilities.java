package uk.co.luke.advancedcomputing.MapReduce.problemspecific.Output;

public class JsonUtilities {
	public static String print( JsonObject object){
		return print(0, object);
	}
	
	public static String print( JsonArray object){
		return print(0, object);
	}
	
	
	
	private static String print(int numberOfTabs, Object object){
		Class elementClass = object.getClass();
		
		if(elementClass.equals(JsonArray.class)){
			JsonArray array = (JsonArray)object;
			String result =  "[\n";
			if(array.elements.size() >= 1){
				result += print(numberOfTabs++,array.elements.get(0));
			}
			for(int counter = 1; counter < array.elements.size(); counter++){
				result += ',' + print(numberOfTabs++,array.elements.get(counter));
			}
			result += "\n" + getTabsString(numberOfTabs) +"]";
			return result;
		}else if (elementClass.equals(JsonObject.class)){
			JsonObject jsonObject = (JsonObject) object;
			String result  =  "{\n";
			
			for(String key : jsonObject.attributes.keySet()){
				result += getTabsString( numberOfTabs + 1) + key + ": " + print( numberOfTabs++,jsonObject.attributes.get(key)) + ',';
			}
			if(result.length() > 0 && result.charAt(result.length() - 1) == ','){
				result  = result.substring(0, result.length() - 1);
			}
			result += "\n" + getTabsString(numberOfTabs) + "}";
			
		}else if(elementClass.equals(Integer.class)){
			int integer = (Integer)object;
			String result = String.valueOf(integer);
			return result;
		}else if(elementClass.equals(Float.class)){
			float floatingPoint = (Float)object;
			return String.valueOf(floatingPoint);
		}else if (elementClass.equals(String.class)){
			String string = (String)object;
			return "'" + string + "'";
		}
		// otherwise
		return null;
		
		
	}
	
	private static String  getTabsString(int numberOfTabs){
		String tabString = "";
		for (int counter = 0; counter < numberOfTabs; counter++){
			tabString+= '\t';
		}
		return tabString;
	}
	
}
