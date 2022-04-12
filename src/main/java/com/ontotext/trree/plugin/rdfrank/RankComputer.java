package com.ontotext.trree.plugin.rdfrank;

import java.io.File;

import com.ontotext.trree.sdk.PluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ontotext.trree.util.BigDoubleArray;

class RankComputer {
	private long limitStatements = Long.MAX_VALUE;
	private long totalIterations = 10;
	private double dampingFactor = 0.85;
	private double epsilon = 0;
	private double minRank, maxRank;
	private File dataDir = null;
	private int entityBitSize = 32;
	private boolean interrupt = false;

	private Logger Logger = LoggerFactory.getLogger(getClass());

	double getDampingFactor() {
		return dampingFactor;
	}

	void setDampingFactor(double d) {
		dampingFactor = d;
	}

	double getEpsilon() {
		return epsilon;
	}

	void setEpsilon(double e) {
		epsilon = e;
	}

	long getTotalIterations() {
		return totalIterations;
	}

	void setMaxIterations(long n) {
		totalIterations = n;
	}

	long getLimitStatements() {
		return limitStatements;
	}

	void setLimitStatements(long n) {
		limitStatements = n;
	}

	void setEntityBitSize(int entityBitSize) {
		this.entityBitSize = entityBitSize;
	}

	private File getDataDir() {
		return dataDir;
	}

	void setDataDir(File dir) {
		dataDir = dir;
	}

	BigDoubleArray compute(GraphReader gr) {
		// create a table storage to handle the graph data
		long dim = gr.nodeCount() + 1;
		if (getDataDir() == null) {
			throw new IllegalArgumentException("No data dir was configured for rank computer");
		}
		String graphPrefix = getDataDir().getAbsolutePath() + File.separator + "graph";
		TableStorage storage = new TableStorage(graphPrefix, dim, dim, entityBitSize);

		// add the whole graph into that table storage
		Logger.info("Reading repository graph data...");
		long count = 0;
		for (gr.reset(); gr.next();) {
			if (++count % 1000000 == 0) {
				Logger.info("Adding " + count + " of " + gr.size());
			}
			storage.add(gr.getFrom(), gr.getTo());
			if (interrupt) {
				storage.shutDown();
				return null;
			}
		}
		gr.close();

		if (count == 0) {
			storage.shutDown();
			throw new PluginException("Selected filter does not return any statements.");
		}

		Logger.info("Finished reading repository graph data.");

		long t1 = System.currentTimeMillis();
		Logger.info("Computing RDF Rank of graph...");

		long size = gr.nodeCount() + 1;

		BigDoubleArray prevRank = new BigDoubleArray(size);
		BigDoubleArray currRank = new BigDoubleArray(size);

		double resetProbability = (1d - dampingFactor) / size;

		// initialize the process with the value of 1/N
		currRank.fill(1d / size);

		// start RDF Rank iterations
		for (int iter = 0; iter < totalIterations; iter++) {
			minRank = 1.0;
			maxRank = 0;

			double totalRank = 0;
			double danglingRank = 0;

			Logger.info("Executing iteration #" + iter);

			// swap current and previous rank arrays
			BigDoubleArray tempRank = currRank;
			currRank = prevRank;
			prevRank = tempRank;
			// clear the contents of the next-to-fill RDF Rank array
			currRank.fill(0);

			// accumulate RDF Rank for nodes
			for (int idx = 0; idx < size; idx++) {
				TableStorage.Iterator rowIterator = storage.rowIterator(idx);
				long outgoing = rowIterator.size();
				if (outgoing == 0) {
					// nowhere to go, this means we could go anywhere
					// rank is dispatched along the whole graph
					danglingRank += prevRank.get(idx);
				} else {
					// rank is dispatched across the outgoing links
					double rankEmission = prevRank.get(idx) / outgoing;
					while (rowIterator.hasNext()) {
						long index = rowIterator.next();
						currRank.set(index, currRank.get(index) + rankEmission);
					}
				}
			}
			// 'normalize' danglingRank rank:
			danglingRank /= size;
			// add up dangling rank, apply damping factor and add reset probability
			for (int idx = 0; idx < size; idx++) {
				double value = currRank.get(idx);
				value += danglingRank;
				value *= dampingFactor;
				value += resetProbability;
				currRank.set(idx, value);
				// accumulate all ranks to get a normalizing factor
				totalRank += value;
			}
			// normalize rank
			for (int idx = 0; idx < size; idx++) {
				if (currRank.get(idx) < 0) {
					Logger.error("Negative rank detected!");
				}
				double normalized = currRank.get(idx) / totalRank;
				if (normalized < minRank) {
					minRank = normalized;
				} else if (normalized > maxRank) {
					maxRank = normalized;
				}
				currRank.set(idx, normalized);
			}
			// compute accumulated difference with the previous rank
			double delta = 0;
			for (int idx = 0; idx < size; idx++) {
				if (currRank.get(idx) < 0) {
					continue;
				}
				double diff = currRank.get(idx) - prevRank.get(idx);
				if (diff < 0) {
					delta -= diff;
				} else {
					delta += diff;
				}
			}
			Logger.debug("Iteration #" + iter + " is different by " + RankUtils.format(delta));

			if (delta <= epsilon) {
				break;
			}

			if (interrupt) {
				storage.shutDown();
				return null;
			}
		}

		long t2 = System.currentTimeMillis();
		Logger.info("Finished computing RDF Rank in " + ((t2 - t1) / 1000) + "s.");

		// remove graph storage (won't be needed any more)
		storage.shutDown();

		// the last computed iteration gives us result
		return currRank;
	}

	void interrupt() {
		this.interrupt = true;
	}

	double getMinRank() {
		return minRank;
	}

	double getMaxRank() {
		return maxRank;
	}
}
