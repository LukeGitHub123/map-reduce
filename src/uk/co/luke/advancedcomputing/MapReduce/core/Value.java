package uk.co.luke.advancedcomputing.MapReduce.core;

/**
 *
 *
 * This class defines an abstract data type which the mapper and reducer use.
 *
 * @param <Key> The type you want to use for the key.
 */
public abstract class Value<Key> {
	private Key key;
	
	public Value(Key key){
		this.key = key;
	}
	
	public Key getKey(){
		return this.key;
	}
}
