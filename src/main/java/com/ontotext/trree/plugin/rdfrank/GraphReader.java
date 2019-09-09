package com.ontotext.trree.plugin.rdfrank;

/**
 * Abstract graph reader -- provides interface for iterating a graph
 */
abstract class GraphReader implements AutoCloseable {
	long from;
	long to;

	/**
	 * Gets the number of nodes in the graph
	 * 
	 * @return the number of nodes in the graph
	 */
	public abstract long nodeCount();

	/**
	 * Gets the number of edges in the graph
	 * 
	 * @return the number of edges in the graph
	 */
	public abstract long size();

	/**
	 * Resets the reader in the beginning of the graph
	 */
	public abstract void reset();

	/**
	 * Goes forward in the graph iteration
	 * 
	 * @return boolean value telling if there are more edges in the graph
	 */
	public abstract boolean next();

	/**
	 * Gets the source node of the current edge
	 * 
	 * @return the source node of the current edge
	 */
	public long getFrom() {
		return from;
	}

	/**
	 * Gets the destination node of the current edge
	 * 
	 * @return the source node of the current edge
	 */
	public long getTo() {
		return to;
	}

	/**
	 * Closes any opened iterators
	 */
	public abstract void close();
}
