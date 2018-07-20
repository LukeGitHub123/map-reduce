package uk.co.luke.advancedcomputing.MapReduce.core;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * This abstract class implements the basic structure of a reducer thread
 *
 * @param <Key> The type of object used for the key
 * @param <V> The type to be used for the value. This must be a value with a matching type to the key in specified in this generic type.
 */
public abstract class Reducer<Key, V extends Value<Key>> extends Thread implements Combiner<Reducer<Key, V>>{
	protected final ArrayList<V>  values;
	protected final Key key;
	protected Hashtable<Key,ArrayList<V>> output;
	boolean isComplete = false;
	
	/**
	 * @param key The key associated with the list of data.
	 * @param values The list of information which share the same key
	 */
	public Reducer(Key key, ArrayList<V> values){
		this.key = key;
		this.values = values;
		this.output = new Hashtable<Key,ArrayList<V>>();
	}
	
	/**
	 * This abstract function is designed to force the developer to implement this function before instantiation. It'spurpose is to expose the key and values to the use in a function which they can then specify.
	 * @param key The object  used to specify the group the values re associated with
	 * @param values A list of values which need to be processed.
	 */
	protected abstract void reduce(Key key, ArrayList<V> values);
	
	/**
	 * The implementation of the run function just calls the user implemented reduce function and then sets the execute complete flag.
	 */
	@Override
	public void run() {
		this.reduce(key,values);
		isComplete = true;
	}
	
}
