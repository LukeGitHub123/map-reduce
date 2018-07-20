package uk.co.luke.advancedcomputing.MapReduce.problemspecific.hadoop;

import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.*;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.InputException;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Scanner;

/**
 * The controller class is one which is there to controll the over all map-reduce process. From mimicing the distributed file system to initialising threads, all the way to managing the execution of the threads.
 */
public class Controller {
	
	final int NUMBER_OF_MAPPER_THREADS = 10;
	File mainDataFile;
	File airportsDatafile;
	ArrayList<VerboseInputException> inputExceptions;
	Hashtable<String, Airport> airportsLookup;
	ArrayList<FlightMapper> flightMappers;
	
	ArrayList<PassengerFlightDetails> passengerFileErrors;
	ArrayList<Exception> systemExceptions;
	
	public Controller( File mainFile, File airportsDataFile) throws FileNotFoundException{
		// the controller just initialises the variable states to their defaults apart from the two file locations
		this.mainDataFile = mainFile;
		this.airportsDatafile = airportsDataFile;
		
		// if either is not a valid file then throw an error.
		if(!this.mainDataFile.isFile() ){
			throw new FileNotFoundException("\"" + this.mainDataFile.getAbsolutePath() + "\" is not a valid file.");
		}
		if(!this.airportsDatafile.isFile()){
			throw new FileNotFoundException("\"" + this.airportsDatafile.getAbsolutePath() + "\" is not a valid file.");
		}
		this.inputExceptions = new ArrayList<>();
		this.passengerFileErrors = new ArrayList<>();
		this.systemExceptions = new ArrayList<>();
		this.flightMappers = new ArrayList<>();
	}
	
	/**
	 * The setup method mimics the functionality of the distributed file system and creates the mapper threads
	 * @throws FileNotFoundException an exception is thrown if the data files passes it are not actually files.
	 */
	public void setup() throws FileNotFoundException {
		Scanner input = new Scanner(this.mainDataFile);
		// construct the output writers for the part files in the distributes file system.
		PrintWriter[] printWriters = new PrintWriter[this.NUMBER_OF_MAPPER_THREADS];
		File[] files = new File[this.NUMBER_OF_MAPPER_THREADS];
		for(int x = 0; x < NUMBER_OF_MAPPER_THREADS; x++){
			files[x] = new File("mapper" + x +"Data.csv");
			printWriters[x] = new PrintWriter(files[x]);
		}
		
		// read in the main data file (passenger data)
		int counter = 0;
		while (input.hasNextLine()){
			// trim has been added to try and remove an error where by a special character was leading the file input
			String fileLine =input.nextLine().trim();
			char c = fileLine.charAt(0);
			String fileLineToUse = null;
			// check if the first character is a spacial character
			if(c == '\uFEFF'){
				fileLineToUse = fileLine.substring(0);
			}else{
				fileLineToUse = fileLine;
			}
			// write the current line into the currently selected file
			printWriters[counter].println(fileLineToUse);
			// increment the current file output
			counter++;
			// if the file output index is more than or equal to the number of output files
			if(counter >= printWriters.length){
				counter = 0;
			}
		}
		// close the input file
		input.close();
		// close all of the output file streams
		for (PrintWriter writer : printWriters){
			writer.close();
		}
		
		// reading in the lookup file
		Scanner airportsFileInput = new Scanner(this.airportsDatafile);
		
		airportsLookup = new Hashtable<>();
		// loop over each line in the airports data file
		while (airportsFileInput.hasNextLine()){
			String fileLine = airportsFileInput.nextLine();
			// split the csv file where commas are encountered
			String[] fileLineParts = fileLine.split(",");
			// check to see if there are 4 columns or not
			if (fileLineParts.length != 4){
				ArrayList<String> errorReasons = new ArrayList<>();
				errorReasons.add("File row contains missing columns.");
				this.inputExceptions.add(new VerboseInputException(new InputException("File Format", errorReasons), fileLine, "Airports"));
				continue; // no point continuing for this line
			}
			// try and construct an airport object which handles validation
			Airport airport = null;
			try {
				airport = new Airport(fileLineParts[1],fileLineParts[0],fileLineParts[1], fileLineParts[2], fileLineParts[3]);
			} catch (InputException e) { // error thrown if input is invalid
				// add the error to the list of ones to report
				this.inputExceptions.add( new VerboseInputException(e, fileLine, "Airports"));
				continue;
			}
			// check to see if the airport has already been encountered
			if(airportsLookup.containsKey(fileLineParts[1])){
				// if the airport is not equal then there is an uncorrectable problem
				if (!airportsLookup.get(fileLineParts[2]).equals(airport)){
					ArrayList<String> errorReasonList = new ArrayList<>();
					errorReasonList.add("More than one airport has been discovered with the same three digit code. Yet the details about them are different");
					errorReasonList.add("The airport code causing the error is " + fileLineParts[1] + '.');
					
					InputException inputException = new InputException("Airport", errorReasonList );
					this.inputExceptions.add(new VerboseInputException(inputException, fileLine, "Airports" ));
				}
				// otherwise do nothing as the object already exists
			}else{
				airportsLookup.put(fileLineParts[1],airport );
			}
			
		}
		airportsFileInput.close(); // close the file
		// spin up the number of mappers specified at the top of this file
		for (int x = 0 ; x < files.length; x++){
			try {
				this.flightMappers.add(new FlightMapper(files[x].getAbsolutePath(),airportsLookup));
			} catch (Exception e) {
				e.printStackTrace();
				this.systemExceptions.add(e);
			}
		}
		
	}
	
	/**
	 * The execute method is intended to be called after the setup method has been called and runs the mapper threads, shuffles the output and then constructs and executes the reducer threads. This method also outputs the results and errors to file.
	 */
	public void execute() {
		// start all the mappers
			for(int x = 0; x < this.flightMappers.size(); x++){
				this.flightMappers.get(x).start();
			}
			// join all of the mappers (wait for them all to complete)
			for(int x = 0 ; x < this.flightMappers.size() ; x ++){
				try {
					this.flightMappers.get(x).join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			// after this point all the threads have finished
		
		// now shuffle the buckets so that all the of the same key are in one bucket
		
		Hashtable<String, ArrayList<PassengerFlightDetails>> mappedData = Utility.shuffle(this.flightMappers);
		
		// pull out all of the errors
		if(mappedData.containsKey("#ERROR#")){
			passengerFileErrors.addAll(mappedData.get("#ERROR#"));
		}
		// construct reducers for all keys but the error code
		ArrayList<FlightReducer> flightReducers = new ArrayList<>();
		for(String key : mappedData.keySet()){
			if(key.equals("#ERROR#")){
				continue;
			}
			// otherwise create a reducer for data with the key
			ArrayList<PassengerFlightDetails> currentEntry = mappedData.get(key);
			
			flightReducers.add(new FlightReducer(key, currentEntry,this.airportsLookup));
		}
		// loop over all of the reducers and start them
		for(FlightReducer reducer : flightReducers){
			
			reducer.start();
			
		}
		// loop over all of the reducers and wait for them to finish before preceding.
		for (FlightReducer reducer : flightReducers){
			try {
				reducer.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/*
		 now all reducer threads have finished
		*
		* */
		// combine all of the reducer states
		FlightReducer combinedReducer = null;
		// Combine Reducers
		if(flightReducers.size() >= 1){
			combinedReducer  = flightReducers.get(0);
		}
		for(int counter = 1; counter < flightReducers.size(); counter++){
			combinedReducer = (FlightReducer) combinedReducer.combine(combinedReducer, flightReducers.get(counter));
		}
		
		
		/*solve problem A
		*
		* Determine the number of flights from each airport; include a list of any airports not used
		* */
		ArrayList<AirportVisit> airportDepartureVisits = combinedReducer.getNumberOfFlightsFromAirports();
		
		ArrayList<Airport> airportsNotDepartedFrom = combinedReducer.getUnvisitedAirports();
		
		File objective1File = new File("objective1.txt");
		
		try {
			// construct an output stream for the objective
			PrintWriter objective1Writer = new PrintWriter(objective1File);
			// print heading
			objective1Writer.println("Number of Airport Departures\n\n");
			// loop over all of the airport visits
			for(AirportVisit visit : airportDepartureVisits){
				Airport airport = visit.getAirport();
				objective1Writer.println("Airport: " + airport.getName() + "[" + airport.getAirportCode() + "] --> (" + airport.getLattitude()+ ","+ airport.getLongitude() +")");
				objective1Writer.println("Number of Departures: " + visit.getNumber() + "\n");
				
			}
			// print out heading
			objective1Writer.println("\n\nAirports not departed From\n\n");
			// loop over airports not departed from
			for (Airport airport : airportsNotDepartedFrom){
				objective1Writer.println(airport.getName() + "[" + airport.getAirportCode() + "] --> (" + airport.getLattitude()+ ","+ airport.getLongitude() +")\n");
			}
			// close the output file
			objective1Writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		/* solve problem B
		*
		* Create a list of flights based on Flight ID, this output should include
		* the passengerID, relevant airport codes, departure time, the arrival time (time in HH:MM:SS format)
		* and flight times
		*
		*
		*  solve Problem C
		*
		* Calculate the number of passengers on each flight
		*
		*
		* */
		// get the list of flights
		ArrayList<Flight> flights = combinedReducer.getFlights();
		// prepare file for second and third  objective
		File objective2File = new File("objective2And3.txt");
		try {
			PrintWriter objective2Writer = new PrintWriter(objective2File);
			// loop over all of the flights
			for(Flight flight : flights){
				objective2Writer.println("Flight ID: " + flight.getFlightID());
				objective2Writer.println("Departure Airport Code: " + flight.getDepartureAirportCode());
				objective2Writer.println("Arrival Airport Code: " + flight.getDestinationAirportCode());
				objective2Writer.println("Departure Time: " + flight.getDepartureTime());
				objective2Writer.println("Arrival Time: " + flight.getArrivalTime());
				
				
				long timeOfFlightInMiliseconds = flight.getTimeOfFlightInMiliseconds();
				long timeOfFlightInMinutes = timeOfFlightInMiliseconds / (1000 * 60);
				objective2Writer.println("Flight Time: " + timeOfFlightInMinutes +" mins" );
				
				ArrayList<String> pasengerIDs = flight.getPassengerIDs();
				// output the number of passengers on flight
				objective2Writer.println("Number of Passengers: " + pasengerIDs.size());
				objective2Writer.println("PassengerID's:");
				// loop over all passenger ID's
				for (String passengerID : pasengerIDs){
					objective2Writer.println(passengerID);
				}
				objective2Writer.println("\n\n");
			}
			// close the file stream for the objective
			objective2Writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		/* Report Errors
		*
		* */
		// construct output file stream for the errors
		File errorFile = new File("Errors.txt");
		try {
			PrintWriter errorWriter = new PrintWriter(errorFile);
			errorWriter.println("///////Mapper Errors///////\n\n");
			// loop over all mapper errors
			for( PassengerFlightDetails details: passengerFileErrors){
				VerboseInputException exception = details.getException();
				errorWriter.println("File Type: " + exception.getFileType());
				errorWriter.println("File Line: " + exception.getFileLine());
				errorWriter.println("Input Name: " + exception.getInputName());
				errorWriter.println("Errors: " + exception.getReasons());
				errorWriter.println("\n\n");
			}
			
			errorWriter.println("\n\n///////Reducer Errors///////\n\n");
			ArrayList<InputException> reducerExceptions = combinedReducer.getExceptions();
			// loop over all reducer errors
			for (InputException inputException: reducerExceptions){
				errorWriter.println("InputName: " +  inputException.getInputName());
				errorWriter.println("Errors: " +  inputException.getReasons());
				errorWriter.println("\n\n");
			}
			// loop over other errors
			errorWriter.println("\n\n///////Other Input Errors///////\n\n");
			for( VerboseInputException e :this.inputExceptions){
				errorWriter.println("File Type: " + e.getFileType());
				errorWriter.println("File Line: " + e.getFileLine());
				errorWriter.println("InputName: " +  e.getInputName());
				errorWriter.println("Errors: " +  e.getReasons());
				errorWriter.println("\n\n");
			}
			// loop over all system errors
			errorWriter.println("\n\n///////System Errors ///////\n\n");
			for ( Exception e: this.systemExceptions){
				errorWriter.println("Error Message: " +  e.getMessage());
				errorWriter.println("Error Cause: " +  e.getCause());
				errorWriter.println("Stack Trace: " +  e.getStackTrace());
				errorWriter.println("\n\n");
			}
			// close errors file
			errorWriter.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	
	
}
