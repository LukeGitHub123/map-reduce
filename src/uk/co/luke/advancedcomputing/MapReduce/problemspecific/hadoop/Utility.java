package uk.co.luke.advancedcomputing.MapReduce.problemspecific.hadoop;

import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.PassengerFlightDetails;

import java.util.ArrayList;
import java.util.Hashtable;


public class Utility {
	/**
	 * This function is a utility function for combining the results of the mapper threads, which are buckets of data associated with the same key.
	 * So if you have two mappers which have generated values with the same key then the array lists are combined and added to the resulting data structure with the same key.
	 * Then if a key exists in one mapper but not the other then the list is added directly to the output.
	 *
	 * @param mappers A list of mapper threads
	 * @return A hashable containing array lists of values which share the same key
	 */
	public static Hashtable<String, ArrayList<PassengerFlightDetails>> shuffle(ArrayList<FlightMapper> mappers){
		// construct the return data structure which matches the type returned by the mappers
		Hashtable<String,  ArrayList<PassengerFlightDetails>> result = new Hashtable<>();
		// loop over all of the mappers
		for(FlightMapper mapper : mappers){
			// get the output of the current mapper thread
			Hashtable<String, ArrayList<PassengerFlightDetails>> mapperOutput = mapper.getOutput();
			// loop over all the bucket keys which exist
			for (String key : mapperOutput.keySet()){
				// if the result data structure already contains the key then combine the contents of the two arraylists
				if(result.containsKey(key)){
					result.get(key).addAll( mapperOutput.get(key));
				}else{ // otherwise the key does not exist in the resulting structure
					result.put(key, mapperOutput.get(key));
				}
			}
		}
		return result;// return the resulting Hashtable
	}
}
