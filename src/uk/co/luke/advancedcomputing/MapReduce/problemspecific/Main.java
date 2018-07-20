package uk.co.luke.advancedcomputing.MapReduce.problemspecific;

import uk.co.luke.advancedcomputing.MapReduce.problemspecific.hadoop.Controller;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * The main class is the point from which the map reduce is started
 */
public class Main {
	/**
	 * This method is the entry point for the map reduce operation
	 * @param mainFileName the main passenger file
	 * @param lookupFileName the airports file
	 */
	public static void mapReduce(File mainFileName, File lookupFileName ){
		File passengersFile = mainFileName;
		File airportsDataFile = lookupFileName;
		Controller mapReduceController = null;
		try { // construct the controller and set it up
			mapReduceController  = new Controller(passengersFile, airportsDataFile);
			mapReduceController.setup();
		} catch (FileNotFoundException e) { // if there has been an error log it and exit
			e.printStackTrace();
			return;
		}
		// start the map reduce operation
		mapReduceController.execute();
		System.out.println("Please check the files created relative to this executable for the program output.");
		
	}
	
}
