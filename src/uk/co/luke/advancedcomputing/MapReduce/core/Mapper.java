package uk.co.luke.advancedcomputing.MapReduce.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Scanner;

/**
 * his class acts as the abstract mapper class with functionality that all implementations of mappers are going to have to do. Primarily reading from file and sending each line into an individual call of the map function..
 *
 * @param <Key> The type used for the key
 * @param <V> The type of value. It must extend the Value class and have a matching value type
 */
public abstract class Mapper<Key, V extends Value<Key>> extends Thread  {
	Scanner fileInput;
	Hashtable<Key,ArrayList<V>> output;
	// flag which can be used to verify that the thread has finished execution without just in case some random error happens.
	boolean isComplete = false;
	
	/**
	 *
	 * @param filePath is the fully qualified file path for the file you wish the mapper handle. e.g C:\\folder\folder\folder\file.csv
	 * @throws Exception used to signify to the constructing code that the specified file is not a valid file.
	 */
	public Mapper(String filePath) throws Exception{
		// construct the file and check that it is a valid file. If not report this to the calling code.
		File file = new File(filePath);
		if(file.isFile()){
			fileInput = new Scanner(file);
		}else{
			throw new Exception("Not a file!");
		}
		output = new Hashtable<Key, ArrayList<V>>();
	}
	
	/**
	 * This function is called on every line in the file the mapper has been tasked with processing. It is abstract in order to force it to be overridden in a subclass before instantiation.
	 *
	 * @param line is the currently read in line of the file.
	 * @return returns the type specified in the specific implementation of the generic class
	 */
	protected abstract V map(String line);
	
	/**
	 * The threads run method is overridden in order to specify the functionality of this thread. fundamentally all this function does is to read in the specified file line by line  and call the map function each time a new line is encountered.
	 */
	@Override
	public void run() {
		// loop over all the lines in the file
		while (fileInput.hasNextLine()){
			// call the map function on the current line of the file
			V value = map(fileInput.nextLine().trim());
			// add the result to the hash table output for the thread
			this.addOutputElement(value.getKey(), value);
		}
		// release the file (resource)
		fileInput.close();
		// signify that the thread has finished execution successfully
		isComplete = true;
		
	}
	
	/**
	 * This function acts as a way of controlling how entries are added to the hash table. It also makes the code more maintainable.
	 * @param key the key to be used in association with the value.
	 * @param value the object containing the value you wish to store.
	 */
	protected void addOutputElement(Key key, V value){
		if(!output.containsKey(key)){
			output.put(key,new ArrayList<V>());
		}
		output.get(key).add(value);
	}
	
	/**
	 * Allows the ability to get access to the result of the mapper.
	 *
	 * @return returns the threads hash table but only if the thread has fully executed.
	 */
	public Hashtable<Key,ArrayList<V>> getOutput(){
		if(isComplete == true){
			return this.output;
		}else {
			return null;
		}
	}
	
}
