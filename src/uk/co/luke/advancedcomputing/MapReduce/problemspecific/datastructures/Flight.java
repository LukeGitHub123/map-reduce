package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

/**
 * The flight class is a data structure for providing convenient access to flight information
 */
public class Flight {
	private String flightID;
	private Hashtable<String, String> passengerIDs;
	private String departureAirportCode;
	private String destinationAirportCode;
	private Date departureTime;
	private Date ArrivalTime;
	
	public Flight(String flightID, String departureAirportCode, String destinationAirportCode, Date departureTime, Date arrivalTime) {
		this.flightID = flightID;
		this.passengerIDs = new Hashtable<>();
		this.departureAirportCode = departureAirportCode;
		this.destinationAirportCode = destinationAirportCode;
		this.departureTime = departureTime;
		ArrivalTime = arrivalTime;
	}
	
	public String getFlightID() {
		return this.flightID;
	}
	
	public void addPassengerID(String passengerID){
		this.passengerIDs.put(passengerID, passengerID);
	}
	
	/**
	 * Converts the hash table into an array list and returns it.
	 * @return returns an array list of passengers
	 */
	public ArrayList<String> getPassengerIDs(){
		ArrayList<String> passengers = new ArrayList<>();
		// iterate over the hash table values
		for (String entry : this.passengerIDs.keySet()) {
			passengers.add(entry);
		}
		return passengers;
	}
	
	public void addPassengerIDs(ArrayList<String> passengerIDs){
		for(String pid : passengerIDs){
			this.passengerIDs.put(pid,pid);
		}
	}
	
	/**
	 *
	 * @param obj the object to compare this object to
	 * @return returns true if the values are equal otherwise returns false
	 */
	@Override
	public boolean equals(Object obj) {
		// check the class of the object being compared
		if(this.getClass() != obj.getClass()){
			return false;
		}
		// cast input to correct type
		Flight flight = (Flight) obj;
		// compare equality of internal variables
		if (flight.flightID.equals( this.flightID) &&
				    flight.departureAirportCode.equals(this.departureAirportCode) &&
				    flight.destinationAirportCode.equals(this.destinationAirportCode) &&
				    flight.ArrivalTime.equals(this.ArrivalTime) &&
				flight.departureTime.equals(this.departureTime)){
			// the passengers are not checked for equality because this is extra details about a flight rather than something unique about the flight.
			return true;
		}
		
		// if the if statement fails then they are not equal
		return false;
	}
	
	
	public String getDepartureAirportCode() {
		return departureAirportCode;
	}
	
	public String getDestinationAirportCode() {
		return destinationAirportCode;
	}
	
	public Date getDepartureTime() {
		return departureTime;
	}
	
	public Date getArrivalTime() {
		return ArrivalTime;
	}
	
	public long getTimeOfFlightInMiliseconds(){
		return  getArrivalTime().getTime() - getDepartureTime().getTime();
	}
}
