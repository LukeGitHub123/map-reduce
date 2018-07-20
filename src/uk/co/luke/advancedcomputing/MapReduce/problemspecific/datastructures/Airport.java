package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

import uk.co.luke.advancedcomputing.MapReduce.core.Value;

import java.util.ArrayList;

/**
 * The Airport class is designed as a data structure for holding and accessing the data from the airport file. It also acts as the validation mechanism.
 */
public class Airport extends Value<String> {
	/*
	* //Airports File
	* Airport name -> X[3..20]
	* Airport IATA/FAA code -> XXX
	* Latitude -> n.n[3..13]
	* Longitude -> n.n[3..13]
	*
	* */
	private String name;
	private String airportCode;
	private float lattitude;
	private float longitude;
	
	/**
	 * The constructor calls the setter methods for all the attributes and reports any exceptions throws.
	 * @param key
	 * @param name
	 * @param airportCode
	 * @param latitude
	 * @param longitude
	 * @throws InputException this exception is thrown if one of the attributes has been deemed to be invalid
	 */
	public Airport(String key, String name, String airportCode, String latitude, String longitude ) throws InputException{
		super(key);
		setName(name);
		setAirportCode(airportCode);
		setLatitude(latitude);
		setLongitude(longitude);
	}
	
	private void setName(String name) throws InputException{
		// declare an array list to hold any detected errors
		ArrayList<String> errors = new ArrayList<>();
		int length = name.length();
		// check the length of the entry to make sure that it is valid
		if(length < 3 || length > 20){
			// add an error to teh array list
			errors.add("The Name of an airport must have a length of between 3 and 20.");
		}
		// loop over all of the characters in the input string
		for(int counter = 0; counter < name.length(); counter++){
			// check if the character is upper case ascii and store the returned string as it will be an error
			String error = consomeUpperCaseCharacter(name.charAt(counter));
			// check to see if there are any errors
			if(error != null){
				// if there are errors then check for the other allowed cases according to Atta before reporting the error
				if(name.charAt(counter) != ' ' && name.charAt(counter) != '/' && name.charAt(counter) != '-'){
					// add errors to the methods error array list
					errors.add(error);
				}
			}
		}
		
		// if the error list is empty then the operation has been successful
		if(!errors.isEmpty()){
			throw new InputException("AirportName", errors);
		}
		// if there are no errors then set the object attribute
		this.name = name;
	}
	private void setAirportCode(String airportCode) throws InputException{
		// declare array to hold generated error strings
		ArrayList<String> errors = new ArrayList<>();
		
		// check the length of the string
		if(airportCode.length() != 3){
			// add relevant error string
			errors.add("Encountered an airport with a code that is not 3 characters long.");
		}
		// loop over all the characters in the string
		for(int counter = 0 ; counter < airportCode.length(); counter++){
			// check that the current character is upper case
			String error  = consomeUpperCaseCharacter(airportCode.charAt(counter));
			if(error != null){
				errors.add(error);
			}
		}
		// check if there are any error messages
		if(!errors.isEmpty()){ // if not empty then throw an error
			throw new InputException("Airport Code", errors);
		}
		// set the object attribute
		this.airportCode = airportCode;
	}
	private void setLatitude(String latitude) throws InputException{
		// call a function for validating coordinates and store the returned list of errors
		ArrayList<String> errors = consumeCoordinate(latitude,2);
		// if there have been errors then report them
		if(!errors.isEmpty()){ // if not empty then raise an error
			throw new InputException("Latitude", errors);
		}
		this.lattitude = java.lang.Float.valueOf(latitude);
	}
	private void setLongitude(String longitude) throws InputException{
		// call a function for validating coordinates and store the returned list of errors
		ArrayList<String> errors = consumeCoordinate(longitude, 3);
		// if there have been errors then report them
		if(!errors.isEmpty()){ // if not empty then raise an error
			throw new InputException("Longitude", errors);
		}
		this.longitude = java.lang.Float.valueOf(longitude);
	}
	
	
	
	
	
	public String getName() {
		return name;
	}
	
	public String getAirportCode() {
		return airportCode;
	}
	
	public float getLattitude() {
		return lattitude;
	}
	
	public float getLongitude() {
		return longitude;
	}
	
	
	/**
	 * This utility function is designed to check if a character conforms to upper case ASCII.
	 *
	 * @param character a character for validating
	 * @return if the input character is not uppercase ASCII then a string will be returned. Otherwise null will be the result.
	 */
	private String consomeUpperCaseCharacter(char character){
		if(!Character.isUpperCase(character)){
			return "'" + character + "' is not an upper case character.";
		}
		return null;
	}
	
	/**
	 * This is a utility function for validating numbers are between 0 and 9.
	 * @param character the character to be analysed
	 * @return null is returned if successful. Otherwise an error string is returned.
	 */
	private String consumeNumber(char character){
		if(Character.isDigit(character)){ // if it is a digit then check its range
			int integer = Character.getNumericValue(character);
			if(integer < 0 || integer > 9){
				return "'" + character + "' has been encountered but it is not a number between  0 and 9 (inclusive).";
			}
			return null;
		}else{
			return "'" + character + "' is not a digit.";
		}
	}
	
	/**
	 * This utility function is for validating
	 * @param coordinate the coordinate value as a string
	 * @param numberOfCharactersBeforeDecimalPoint an integer corresponding to the desired number of decimal laces after the decimal point.
	 * @return an array list of any errors which have been encountered
	 */
	private ArrayList<String> consumeCoordinate(String coordinate, int numberOfCharactersBeforeDecimalPoint){ // [3..13]
		// declare an array list of error strings
		ArrayList<String> errors = new ArrayList<>();
		int length = coordinate.length();
		// check the string contains a decimal point
		if(!coordinate.contains(".")){ // if a dot is not encountered then raise an error
			errors.add("Expected a '.' when reading the value of '" + coordinate + "'");
		}
		// split the string where there is a decimal point
		String[] floatParts = coordinate.split("\\.");
		// check to see if there was more than one decimal point in the string
		if(floatParts.length != 2){
			errors.add("Encountered more than one '.' or none at all.");
		}else{
			String leftHandSide = floatParts[0];
			// get the length of the strings
			int leftHandSideLength = leftHandSide.length();
			String rightHandSide = floatParts[1];
			int rightHandSideLength  = rightHandSide.length();
			// handling the possibility of a negative sign
			
			// check if the first character is a negative sign
			if(leftHandSideLength > 0 && leftHandSide.charAt(0) ==  '-'){
				leftHandSide = leftHandSide.substring(1); // removing the negative sign so it does not affect any of the other validation
				leftHandSideLength = leftHandSide.length(); // update the length
			}
			// check the number of characters before the decimal point
			if(leftHandSideLength > numberOfCharactersBeforeDecimalPoint  || leftHandSideLength < 1 ){
				errors.add("The first part of the value does not have the correct number of characters which is between 1 and " + numberOfCharactersBeforeDecimalPoint + " characters. With an optional extra negative sign (-).");
			}
			//check to see if there are no numbers after the decimal point
			if(rightHandSideLength == 0){
				errors.add("There are no characters after the decimal point.");
			}
		}
		
		return errors; // return the set of errors
	}
}
