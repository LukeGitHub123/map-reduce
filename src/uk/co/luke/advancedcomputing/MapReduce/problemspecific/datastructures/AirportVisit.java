package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

public class AirportVisit {
	protected Airport airport;
	protected int number;
	
	public AirportVisit(Airport airport, int numberOfVisits){
		this.airport = airport;
		this.number = numberOfVisits;
	}
	
	public Airport getAirport() {
		return airport;
	}
	
	public int getNumber() {
		return number;
	}
}
