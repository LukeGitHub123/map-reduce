package uk.co.luke.advancedcomputing.MapReduce.core;

/**
 * This interface is implemented by the reducer class such that it enforces the implementation of a mechanism for combining reducer data together
 *
 * @param <T> Any type of object you want to combine.
 */
public interface Combiner<T> {
	/**
	 *
	 * @param reducer1 the first reducer you wish to combine
	 * @param reducer2 the second reducer you wish to combine
	 * @return
	 */
	T combine(T reducer1, T reducer2);
}
