package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

import java.util.Date;

public class Time {
	private int hours;
	private int mins;
	private int seconds;
	
	public Time(Date date) {
		this.hours = date.getHours();
		this.mins = mins =date.getMinutes();
		this.seconds = date.getSeconds();
	}
	
	public int getHours() {
		return hours;
	}
	
	public int getMins() {
		return mins;
	}
	
	public int getSeconds() {
		return seconds;
	}
}
