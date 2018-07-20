package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

import uk.co.luke.advancedcomputing.MapReduce.core.Value;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

/**
 * PassengerFlightDetails is a class intended to provide convenient access to rows in the flight data file while also validating the data.
 */
public class PassengerFlightDetails extends Value<String> {
	// the data structure can also be an error
	private VerboseInputException exception;
	
	/**
	 * This constructor initialises the object in an error state including updating the key such that it is an error code
	 * @param exception the exception to initialise with
	 * @param lineFromFile the line which caused the error
	 */
	public PassengerFlightDetails(InputException exception, String lineFromFile){
		super("#ERROR#");
		this.exception = new VerboseInputException(exception, lineFromFile,"Passengers");
	}
	
	public VerboseInputException getException() {
		return this.exception;
	}
	
	/*
		* // flights file
		* Passenger ID -> XXXnnnnXXn
		* Flight id ->    XXXnnnnX
		* From Airport FAA/ IATA code -> XXX
		* Destination airport code -> XXX
		* Departure time GMT-> n[10] in epoch time in Linux
		* Total Flight time (mins) -> n [1..4]
		*
		*
		*  X is uppercase ASCII
		* n is a digit 0 -9
		* [n..m] is the min/max range of the number of digits/ characters in a string
		*
		* */
	private String passengerID;
	private String flightID;
	private String instantiatedAirportCode;
	private String destinationAirportCode;
	private Date epochTime;
	private int flightTimeInMinutes;
	
	private Hashtable<String, Airport> allAirports;
	// setters
	
	/**
	 * this is the primary constructor which is called.
	 * @param key
	 * @param passengerID
	 * @param flightID
	 * @param startingAirportCode
	 * @param destinationAirportCode
	 * @param epochTime
	 * @param flightTimeInMinutes
	 * @param allAirports
	 * @throws InputException if an input exception is thrown than the input data is invalid.
	 */
	public PassengerFlightDetails(String key,String passengerID, String flightID, String startingAirportCode,String destinationAirportCode, String epochTime, String flightTimeInMinutes, Hashtable<String,Airport> allAirports) throws InputException {
		super(key);
		this.allAirports = allAirports;
		setpassengerID(passengerID);
		setFlightID(flightID);
		setInstantiatedAirportCode(startingAirportCode);
		setDestinationAirportCode(destinationAirportCode);
		setEpochTime(epochTime);
		setFlightTimeInMinutes(flightTimeInMinutes);
		
		
	}
	
	protected void setpassengerID(String passengerID) throws InputException {
		//Passenger ID -> XXXnnnnXXn
		ArrayList<String> errors = new ArrayList<>();
		int len = passengerID.length();
		char[] chars = passengerID.toCharArray();
		// check the length of the input string
		if(passengerID.length() != 10) {
			errors.add("The length should be 10 characters/digits long. A length of " + passengerID.length() + " has been encountered.");
		}
		// asuming the length of the input string is more than 3
		if(passengerID.length() >= 3){ // make sure it is long enough to do this check.
			// for the first 3 characters see if they are valid
			for(int counter = 0; counter < 3; counter++){
				String err = this.consumeUpperCaseAsciiCharacter(passengerID.charAt(counter));
				if (err != null){
					errors.add(err);
				}
			}
		}
		// now handling the 4 n values
		if(passengerID.length() >= 7){
			for (int counter = 3; counter < 7; counter++){
				String err = this.consumeNumber(passengerID.charAt(counter));
				if (err != null){
					errors.add(err);
				}
			}
		}
		// now the last two ascii characters are examined
		if(passengerID.length() >= 9){
			for (int counter = 7; counter < 9; counter++){
				String err = this.consumeUpperCaseAsciiCharacter(passengerID.charAt(counter));
				if(err != null){
					errors.add(err);
				}
			}
		}
		// now examine the final digit at the end of the string.
		if(passengerID.length() == 10){
			String err = this.consumeNumber(passengerID.charAt(9));
			if(err != null ){
				errors.add(err);
			}
		}
		
		if(!errors.isEmpty()){
			throw new InputException("Passenger ID", errors);
		}
		this.passengerID = passengerID;
	}
	
	protected void setFlightID(String flightID) throws InputException{
		//Flight id ->    XXXnnnnX
		ArrayList<String> errors = new ArrayList<>();
		if(flightID.length() != 8) {
			errors.add("The length should be 8 characters/digits long. A length of " + flightID.length() + " has been encountered.");
		}
		// if the length of the string is at least 3
		if(flightID.length() >= 3){ // make sure it is long enough to do this check.
			// for the first 3 characters see if they are valid
			for(int counter = 0; counter < 3; counter++){
				String err = this.consumeUpperCaseAsciiCharacter(flightID.charAt(counter));
				if (err != null){
					errors.add(err);
				}
			}
			
		}
		// now handling the 4 n values
		if(flightID.length() >= 7){
			for (int counter = 3; counter < 7; counter++){
				String err = this.consumeNumber(flightID.charAt(counter));
				if (err != null){
					errors.add(err);
				}
			}
		}
		// now the last two ascii characters are examined
		if(flightID.length() == 8){
			String err = this.consumeUpperCaseAsciiCharacter(flightID.charAt(7));
			if(err != null){
				errors.add(err);
			}
		}
		
		if(!errors.isEmpty()){
			throw new InputException("Flight ID", errors);
		}
		this.flightID = flightID;
	}
	
	protected void setInstantiatedAirportCode(String instantiatedAirportCode) throws InputException{
		//From Airport FAA/ IATA code -> XXX
		
		ArrayList<String> errors = this.consumeAirportCodes(instantiatedAirportCode);
		// if not empty then errors have occurred
		if (!errors.isEmpty()){
			throw new InputException("Starting Airport Code", errors);
		}
		this.instantiatedAirportCode = instantiatedAirportCode;
	}
	
	protected void setDestinationAirportCode(String destinationAirportCode) throws InputException{
		// Destination airport code -> XXX
		
		ArrayList<String> errors = this.consumeAirportCodes(destinationAirportCode);
		// if not empty then errors have occurred
		if (!errors.isEmpty()){
			throw new InputException("Destination Airport Code", errors);
		}
		this.destinationAirportCode = destinationAirportCode;
	}
	
	protected void setEpochTime(String epochTime) throws InputException{
		//Departure time GMT-> n[10] in epoch time in Linux
		
		ArrayList<String> errors = new ArrayList<>();
		
		if (epochTime.length() != 10){
			errors.add("The input value should be 10 digits long. Instead '" + epochTime.length() + "' digits were encountered.");
		}
		
		if (!errors.isEmpty()){
			throw new InputException("Departure Time (Epoch Time)",errors);
		}
		this.epochTime = new Date(Long.valueOf(epochTime) * 1000); // TODO: remove the multiplication if the epoch time is already in milliseconds
	}
	
	protected void setFlightTimeInMinutes(String flightTimeInMinutes) throws InputException{
		//Total Flight time (mins) -> n [1..4]
		ArrayList<String> errors = new ArrayList<String>();
		
		if(flightTimeInMinutes.length() < 1 ){
			errors.add("The value must contain at least 1 digit.");
		}
		if(flightTimeInMinutes.length() > 4){
			errors.add("The value must be no longer than 4 digits. Instead '" + flightTimeInMinutes.length() + "' was encountered.");
		}
		// check the characters recieved to make sure they are digits
		for( char character : flightTimeInMinutes.toCharArray()){
			String err = this.consumeNumber(character);
			if (err != null){
				errors.add(err);
			}
		}
		
		if(! errors.isEmpty()){
			throw new InputException("Flight Time In Minutes", errors);
		}
		
		
		this.flightTimeInMinutes = Integer.valueOf( flightTimeInMinutes);
	}
	
	// getters
	public String getpassengerID() {
		return passengerID;
	}
	
	public String getFlightID() {
		return flightID;
	}
	
	public String getInstantiatedAirportCode() {
		return instantiatedAirportCode;
	}
	
	public String getDestinationAirportCode() {
		return destinationAirportCode;
	}
	
	public Date getEpochTime() {
		return epochTime;
	}
	
	public int getFlightTimeInMinutes() {
		return flightTimeInMinutes;
	}
	
	// this method returns an error string or null if successful.
	private String consumeUpperCaseAsciiCharacter(char character){
		String error = null;
		
		if(!Character.isAlphabetic(character)){
			error = "The first three character should only be ASCII characters.";
		}
		if(!Character.isUpperCase(character)){
			error = "The first three characters should all be upper case characters.";
		}
		
		return error;
	}
	
	private String consumeNumber(char character){
		if(!Character.isDigit(character)){
			return "Only a digit from 0 to 9 is expected. Instead '" + character + "' has been encountered.";
		}
		return null;
	}
	
	private ArrayList<String> consumeAirportCodes(String code){
		ArrayList<String> errors = new ArrayList<>();
		// check the length
		if(code.length() != 3){
			errors.add("Invalid length. A length of '" + code.length() + "' has been encountered.");
		}
		// loopover all the characters in the string
		for (char character : code.toCharArray()){
			String err = this.consumeUpperCaseAsciiCharacter(character);
			if(err != null){
				errors.add(err);
			}
		}
		// check to see if the string is a valid airport code
		if(!this.allAirports.containsKey(code)){ // does not contain airport abbreviation
			errors.add("The airport code '" + code + "' does not exist.");
		}
		return errors;
	}
	
	public Date getDepartureTime(){
		return epochTime;
	}
	public Date getArrivalTime(){
		Date date = new Date(epochTime.getTime() + (this.flightTimeInMinutes * 60000));
		return date;
	}
}
