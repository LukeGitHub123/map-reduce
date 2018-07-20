package uk.co.luke.advancedcomputing.MapReduce.problemspecific.hadoop;

import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.Airport;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.InputException;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.PassengerFlightDetails;
import uk.co.luke.advancedcomputing.MapReduce.core.Mapper;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The FlightMapper class is an implementation of the mapper for solving the three objectives.
 */
public class FlightMapper extends Mapper<String, PassengerFlightDetails> {
	private Hashtable<String, Airport> allAirports;
	public FlightMapper(String filePath, Hashtable<String, Airport> allAirports) throws Exception {
		super(filePath);
		this.allAirports = allAirports;
	}
	
	/**
	 * This is the implementation of the map function of the base class.
	 * @param line is the currently read in line of the file.
	 * @return
	 */
	@Override
	protected PassengerFlightDetails map(String line) {
		
		// if the first character is a control character then remove it
		String lineToUse = line;
		if(line.length() > 0){
			char firstCharacter  = line.charAt(0);
			if(firstCharacter == '\uFEFF'){
				lineToUse = line.substring(1);
			}
		}
		// split up the csv file into its columns
		String[] lineParts = lineToUse.split(",");
		PassengerFlightDetails details = null;
		
		try { // try constructing and returning a normal object
			//                                   Flight Id as the key (grouping all the data to do with a flight together)
			if(lineParts.length != 6){// if there is a structural error then throw an error
				ArrayList<String> errorMessages = new ArrayList<>();
				errorMessages.add("Invalid number of columns in the current file line. Instead of 6 a value of " + lineParts.length + " has been encountered.");
				throw new InputException("File Format",errorMessages);
			}else{
				details = new PassengerFlightDetails(lineParts[1], lineParts[0], lineParts[1], lineParts[2], lineParts[3], lineParts[4],lineParts[5], allAirports  );
				return details;
			}
			
			
		} catch (InputException e) { // constructing and returning an error
			details = new PassengerFlightDetails(e,lineToUse);
			return details;
		}
	}
	
}
