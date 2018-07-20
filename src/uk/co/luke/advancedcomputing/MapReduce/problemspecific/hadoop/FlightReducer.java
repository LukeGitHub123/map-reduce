package uk.co.luke.advancedcomputing.MapReduce.problemspecific.hadoop;

import uk.co.luke.advancedcomputing.MapReduce.core.Reducer;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.InputException;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.Airport;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.AirportVisit;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.Flight;
import uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures.PassengerFlightDetails;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

/**
 * The flight reducer class is intended to carry out the objective specific reducer.
 */
public class FlightReducer extends Reducer<String, PassengerFlightDetails> {
	private Hashtable<String, Airport> allAirports;
	private Hashtable<String,Integer> numberOfAirportVisits;
	private Hashtable<String, Flight> flights;
	
	private ArrayList<InputException> exceptions;
	
	public FlightReducer(String s, ArrayList<PassengerFlightDetails> values, Hashtable<String, Airport> allAirports) {
		super(s, values);
		this.allAirports = allAirports;
		
		this.numberOfAirportVisits = new Hashtable<>();
		// add all the airport keys to the hash map with a visit number of 0
		for (Airport airport : this.allAirports.values()){
			this.numberOfAirportVisits.put( airport.getKey(), 0); //
		}
		this.exceptions = new ArrayList<>();
		this.flights = new Hashtable<>();
	}
	// increment the number of visits to an airport by 1
	protected void addVisitedAirport(String key){
		this.numberOfAirportVisits.put(key, this.numberOfAirportVisits.get(key) + 1);
	}
	
	private void addFlight(Flight flight){
		// if the flight id does not already exist
		if (!this.flights.containsKey(flight.getFlightID())){
			this.flights.put(flight.getFlightID(), flight);
			addVisitedAirport(flight.getDepartureAirportCode());
		}else{ // if the flight id does exist
			if(! this.flights.get(flight.getFlightID()).equals(flight)){ // check to see if the flights not are equal. If they are not then report an error
				ArrayList<String> errorReasons = new ArrayList<>();
				errorReasons.add("Two rows have the same flight id but different flight information. The latter details have been ignored");
				this.exceptions.add(new InputException("Flight", errorReasons ));
			}
		}
	}
	
	public ArrayList<InputException> getExceptions() {
		return this.exceptions;
	}
	
	public ArrayList<Airport> getUnvisitedAirports(){
		ArrayList<Airport> unvisitedAirportsList = new ArrayList<>();
		// loop over the airport visits hash table
		for (Map.Entry<String, Integer> entry: this.numberOfAirportVisits.entrySet()){
			if(entry.getValue() == 0){ // if its value is 0 then add it to the array to be returned
				unvisitedAirportsList.add(this.allAirports.get(entry.getKey()));
			}
		}
		return unvisitedAirportsList;
	}
	
	public ArrayList<AirportVisit> getNumberOfFlightsFromAirports(){
		ArrayList<AirportVisit> airportVisitInformation = new ArrayList<>();
		// loop over all the entries in the visited airport hash table and convert it to an array list
		for (Map.Entry<String,Integer> entry : this.numberOfAirportVisits.entrySet()){
			airportVisitInformation.add(new AirportVisit(this.allAirports.get(entry.getKey()), entry.getValue()));
		}
		
		return airportVisitInformation;
	}
	
	private void addPassengerToFlight(String flightID, String passsengerID){
		
		this.flights.get(flightID).addPassengerID(passsengerID);
	}
	
	/**
	 * this is the implementation of the reduce function
	 * @param key The object  used to specify the group the values re associated with
	 * @param values A list of values which need to be processed.
	 */
	@Override
	protected void reduce(String key, ArrayList<PassengerFlightDetails> values) {
		// the key is the flight number
		for(PassengerFlightDetails details : values){
			addFlight(new Flight(details.getFlightID(), details.getInstantiatedAirportCode(), details.getDestinationAirportCode(),details.getDepartureTime(), details.getArrivalTime()));
			addPassengerToFlight(details.getFlightID(), details.getpassengerID());
		}
	}
	
	/**
	 * This is an implementation for the combiner of reducers
	 * @param reducer1 the first reducer you wish to combine
	 * @param reducer2 the second reducer you wish to combine
	 * @return
	 */
	@Override
	public Reducer combine(Reducer reducer1, Reducer reducer2) {
		// cast types to their actual type
		FlightReducer flightReducer1 = (FlightReducer) reducer1;
		FlightReducer flightReducer2 = (FlightReducer) reducer2;
		// combine the flights information
		flightReducer1.flights.putAll( flightReducer2.flights);
		// just in case there is more than one thread reducing the same key
		for(Map.Entry<String, Flight> entry : flightReducer2.flights.entrySet()){
			if(flightReducer1.flights.containsKey(entry.getKey())){
				flightReducer1.flights.get(entry.getKey()).addPassengerIDs(entry.getValue().getPassengerIDs());
			}else{
				flightReducer1.flights.put(entry.getKey(), entry.getValue());
			}
		}
		// merge the exceptions
		flightReducer1.exceptions.addAll(flightReducer2.exceptions);
		
		// merge the airport departure visits
		for(Map.Entry<String,Integer> entry : flightReducer2.numberOfAirportVisits.entrySet()){
			flightReducer1.numberOfAirportVisits.put(entry.getKey(),
					flightReducer1.numberOfAirportVisits.get(entry.getKey()) + entry.getValue());
		}
		// return the results
		return flightReducer1;
		
	}
	
	
	public ArrayList<Flight> getFlights(){
		// convert flights hash table to array by looping over hash table
		ArrayList<Flight> flightList = new ArrayList<>();
		for(String key  : this.flights.keySet()){
			flightList.add(this.flights.get(key));
		}
		return flightList;
	}
}
