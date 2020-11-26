package com.ontotext.trree.plugin.rdfrank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.ontotext.trree.sdk.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.ontotext.trree.sdk.Entities.Scope;
import com.ontotext.trree.util.BigDoubleArray;
import com.ontotext.trree.plugin.rdfrank.Configuration.RDFRankProperty;

import static com.ontotext.trree.plugin.rdfrank.RDFRankPlugin.Status.*;
import static com.ontotext.trree.plugin.rdfrank.Configuration.RDFRankProperty.*;

/**
 * RDFRankPlugin - a plug-in that computes and provides access to RDF ranks of the repository entities based
 * on the PageRank algorithm applied on the repository graph. Computation and access to ranks is achieved
 * through system queries defined and interpreted by the plug-in.
 */
public class RDFRankPlugin extends PluginBase implements PatternInterpreter, UpdateInterpreter, RDFRankProvider {
	private static final String STORAGE_FILE = "storage";
	private static final String STATE_FILE = "state";
	private static final String TEMP_SUFFIX = ".temp";

	private static final double DEFAULT_EPSILON = 0.01;
	private static final int DEFAULT_MAX_ITERATIONS = 20;

	private static final IRI RANK_TYPE = SimpleValueFactory.getInstance().createIRI("http://www.w3.org/2001/XMLSchema#float");

	private static Map<IRI, Long> specialGraphsMapping;
	static {
		specialGraphsMapping = new HashMap<>(1);
		specialGraphsMapping.put(SimpleValueFactory.getInstance().createIRI("http://www.openrdf.org/schema/sesame#nil"), -3L);
	}

	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	private FileRankReader rankReader = null;

	private long hasRDFRankID, hasRDFRankID_3, hasRDFRankID_4, hasRDFRankID_5;
	private long setParamID;
	private long maxIterationsID;
	private long epsilonID;
	private long computeID, computeIncrementalID;
	private long computeAsyncID, computeIncrementalAsyncID;
	private long exportID;
	private long statusID, presentID, interruptID;
	private long includedPredicates, includedGraphs, excludedPredicates, excludedGraphs, includeExplicit, includeImplicit, filtering;

	private long contextId = 0;

	private int maxIterations = DEFAULT_MAX_ITERATIONS;
	private double epsilon = DEFAULT_EPSILON;

	private RankComputer computer = null;

	private boolean computationInProgress;
	private boolean interrupt = false;

	private Configuration configuration;
	private Throwable error = null;

	@Override
	public String getName() {
		return "rdfrank";
	}

	@Override
	public void initialize(InitReason initReason, PluginConnection pluginConnection) {
		initializeStandAlone();

		// register our predicates
		Entities entities = pluginConnection.getEntities();
		hasRDFRankID = entities.put(RDFRank.HAS_RDF_RANK, Scope.SYSTEM);
		hasRDFRankID_3 = entities.put(RDFRank.HAS_RDF_RANK_3, Scope.SYSTEM);
		hasRDFRankID_4 = entities.put(RDFRank.HAS_RDF_RANK_4, Scope.SYSTEM);
		hasRDFRankID_5 = entities.put(RDFRank.HAS_RDF_RANK_5, Scope.SYSTEM);
		setParamID = entities.put(RDFRank.SET_PARAM, Scope.SYSTEM);
		maxIterationsID = entities.put(RDFRank.MAX_ITERATIONS, Scope.SYSTEM);
		epsilonID = entities.put(RDFRank.EPSILON, Scope.SYSTEM);
		computeID = entities.put(RDFRank.COMPUTE, Scope.SYSTEM);
		computeIncrementalID = entities.put(RDFRank.COMPUTE_INCREMENTAL, Scope.SYSTEM);
		computeAsyncID = entities.put(RDFRank.COMPUTE_ASYNC, Scope.SYSTEM);
		computeIncrementalAsyncID = entities.put(RDFRank.COMPUTE_ASYNC_INCREMENTAL, Scope.SYSTEM);
		exportID = entities.put(RDFRank.EXPORT, Scope.SYSTEM);
		statusID = entities.put(RDFRank.STATUS, Scope.SYSTEM);
		presentID = entities.put(RDFRank.PRESENT, Scope.SYSTEM);
		interruptID = entities.put(RDFRank.INTERRUPT, Scope.SYSTEM);

		filtering = entities.put(RDFRank.FILTERING, Scope.SYSTEM);
		includedPredicates = entities.put(RDFRank.INCLUDED_PREDICATES, Scope.SYSTEM);
		includedGraphs = entities.put(RDFRank.INCLUDED_GRAPHS, Scope.SYSTEM);
		excludedPredicates = entities.put(RDFRank.EXCLUDED_PREDICATES, Scope.SYSTEM);
		excludedGraphs = entities.put(RDFRank.EXCLUDED_GRAPHS, Scope.SYSTEM);
		includeExplicit = entities.put(RDFRank.INCLUDE_EXPLICIT, Scope.SYSTEM);
		includeImplicit = entities.put(RDFRank.INCLUDE_IMPLICIT, Scope.SYSTEM);

		contextId = entities.put(RDFRank.CONTEXT, Scope.SYSTEM);
	}

	private void initializeStandAlone() {
		// prepare to read the ranks from binary file
		rankReader = new FileRankReader(getStorageFile());
		getDataDir().mkdirs();

		configuration = new Configuration(getStateFile());
		configuration.initialize();
	}

	@Override
	public void shutdown(ShutdownReason shutdownReason) {
		executor.shutdown();
	}

	@Override
	public StatementIterator interpret(long subject, long predicate, long object, long context,
									   PluginConnection pluginConnection, RequestContext requestContext) {
		// check if this is some operation
		boolean result = interpretOperations(subject, predicate, object, pluginConnection);
		if (result)
			return StatementIterator.TRUE();

		// check if this is a rank query
		if (Utils.match(predicate, hasRDFRankID, hasRDFRankID_3, hasRDFRankID_4, hasRDFRankID_5)) {
			return interpretHasRDFRank(subject, predicate, pluginConnection.getEntities());
		}
		// rank export
		if (Utils.match(predicate, exportID)) {
			try {
				exportRank(Utils.getString(pluginConnection.getEntities(), object), pluginConnection.getEntities());
			} catch (IOException e) {
				getLogger().error("Export failed: " + e.getMessage());
				return StatementIterator.FALSE();
			}
			return StatementIterator.TRUE();
		}
		// status query
		if (Utils.match(predicate, statusID)) {
			Status status = getStatus(pluginConnection.getEntities());
			String statusString = status.toString();
			if (status == Status.ERROR) {
				assert(error != null);
				statusString = statusString + " " + error.getMessage();
			}

			long statusRequestEntity = pluginConnection.getEntities().put(SimpleValueFactory.getInstance()
														.createLiteral(statusString), Entities.Scope.REQUEST);
			return StatementIterator.create(subject, predicate, statusRequestEntity, context);
		}
		// present query. Basically checks that the plugin is initialized
		if (Utils.match(predicate, presentID)) {
			return StatementIterator.TRUE();
		}
		// Filter mode controls
		if (Utils.match(predicate, filtering)) {
			return configuration.getFilteringEnabled() ? StatementIterator.TRUE() : StatementIterator.FALSE();
		}
		if (Utils.match(predicate, includeExplicit)) {
			return configuration.getIncludeExplicit() ? StatementIterator.TRUE() : StatementIterator.FALSE();
		}
		if (Utils.match(predicate, includeImplicit)) {
			return configuration.getIncludeImplicit() ? StatementIterator.TRUE() : StatementIterator.FALSE();
		}
		if (Utils.match(predicate, includedPredicates, includedGraphs, excludedPredicates, excludedGraphs)) {
			return getFilterList(predicate, pluginConnection.getEntities());
		}

		return null;
	}

	@Override
	public long[] getPredicatesToListenFor() {
		return new long[] { setParamID, computeID, computeIncrementalID, computeAsyncID, computeIncrementalAsyncID, interruptID, includedPredicates,
				includedGraphs, excludedPredicates, excludedGraphs, includeExplicit, includeImplicit };
	}

	@Override
	public boolean interpretUpdate(long subject, long predicate, long object, long context, boolean isAddition,
			boolean isExplicit, PluginConnection pluginConnection) {
		interpretOperations(subject, predicate, object, pluginConnection);
		return true;
	}

	private boolean interpretOperations(long subject, long predicate, long object, PluginConnection pluginConnection) {
		// check if this is our parameter configuration
		if (Utils.match(predicate, setParamID)) {
			if (Utils.match(subject, maxIterationsID)) {
				setMaxIterations(Utils.getInteger(pluginConnection.getEntities(), object));
				return true;
			}
			if (Utils.match(subject, epsilonID)) {
				setEpsilon(Utils.getFloat(pluginConnection.getEntities(), object));
				return true;
			}
			if (Utils.match(subject, filtering)) {
				configuration.setFilteringEnabled(Utils.getBoolean(pluginConnection.getEntities(), object));
				configuration.save();
				return true;
			}
			if (Utils.match(subject, includeExplicit)) {
				configuration.setIncludeExplicit(Utils.getBoolean(pluginConnection.getEntities(), object));
				configuration.save();
				return true;
			}
			if (Utils.match(subject, includeImplicit)) {
				configuration.setIncludeImplicit(Utils.getBoolean(pluginConnection.getEntities(), object));
				configuration.save();
				return true;
			}
			// not our parameter
			return false;
		}
		// rank re-computation
		if (Utils.match(predicate, computeID)) {
			recomputeRankSync(pluginConnection);
			return true;
		}
		if (Utils.match(predicate, computeIncrementalID)) {
			recomputeIncrementalRankSync(pluginConnection);
			return true;
		}
		// For async computation
		if (Utils.match(predicate, computeAsyncID)) {
			if (pluginConnection.isWorkerAttached()) {
				// Should let user be aware that asynchronous computations are not allowed in cluster mode
				getLogger().warn("Asynchronous computation is not allowed in cluster mode. Computing RDF rank synchronously");
				recomputeRankSync(pluginConnection);
				return true;
			}
			recomputeRankAsync(pluginConnection);
			return true;
		}
		if (Utils.match(predicate, computeIncrementalAsyncID)) {
			if (pluginConnection.isWorkerAttached()) {
				getLogger().warn("Asynchronous incremental computation is not allowed in cluster mode. Computing RDF rank synchronously");
				recomputeIncrementalRankSync(pluginConnection);
				return true;
			}
			recomputeIncrementalRankAsync(pluginConnection);
			return true;
		}
		// interrupt computation
		if (Utils.match(predicate, interruptID)) {
			computationInterrupt(pluginConnection.getEntities());
			return true;
		}
		// Add/remove elements from the filter lists
		if (Utils.match(predicate, includedPredicates, includedGraphs, excludedPredicates, excludedGraphs)) {
			// These predicates can be used to get the value of the lists (therefore no work here)
			if (object == 0) {
				return false;
			}
			String operation = pluginConnection.getEntities().get(object).stringValue();
			if ("add".equals(operation)) {
				addToFilterList(subject, predicate, pluginConnection.getEntities());
				return true;
			}
			if ("remove".equals(operation)) {
				removeFromFilterList(subject, predicate, pluginConnection.getEntities());
				return true;
			}
			return false;
		}
		return false;
	}

	private StatementIterator interpretHasRDFRank(final long subjectPattern, final long predicatePattern,
												  final Entities entities) {

		final long minId = subjectPattern == 0 ? 1 : subjectPattern;
		final long maxId = subjectPattern == 0 ? entities.size() : subjectPattern;

		return new StatementIterator() {
			int digits = 2;
			{
				subject = minId - 1;
				predicate = predicatePattern;
				if (predicatePattern == hasRDFRankID_3)
					digits = 3;
				else if (predicatePattern == hasRDFRankID_4)
					digits = 4;
				else if (predicatePattern == hasRDFRankID_5)
					digits = 5;
			}

			@Override
			public boolean next() {
				for (long id = subject + 1; id <= maxId; id++) {
					Entities.Type entityType = entities.getType(id);
					if (entityType == Entities.Type.URI || entityType == Entities.Type.BNODE || entityType == Entities.Type.TRIPLE) {
						subject = id;
						String rankString = getFormattedRank(subject, this.digits);
						Literal rankLiteral = SimpleValueFactory.getInstance().createLiteral(rankString, RANK_TYPE);
						object = entities.put(rankLiteral, Scope.REQUEST);
						return true;
					}
				}
				return false;
			}

			@Override
			public void close() {
				subject = maxId;
			}
		};
	}

	@Override
	public double getRank(long id) {
		return rankReader.read(id);
	}

	@Override
	public double getNormalizedRank(long id) {
		double number = getRank(id);
		double[] thresholds = getThresholds();
		double rank = 0;
		int index = RankUtils.findThresholdIndex(number, thresholds);
		if (index >= 0) {
			double min = thresholds[index];
			double max = thresholds[index + 1];
			rank = (min == max) ? 1 : ((number - min) / (max - min) + index / 2) * (2.0 / thresholds.length);
		}

		return rank;
	}

	private String getFormattedRank(long id) {
		return getFormattedRank(id, 2);
	}

	private String getFormattedRank(long id, int digits) {
		return RankUtils.formatWithDigits(getNormalizedRank(id), digits);
	}

	private double[] getThresholds() {
		return rankReader.getThresholds();
	}

	private int getMaxIterations() {
		return maxIterations;
	}

	private void setMaxIterations(int value) {
		maxIterations = value;
	}

	private double getEpsilon() {
		return epsilon;
	}

	private void setEpsilon(double value) {
		epsilon = value;
	}

	private synchronized void recomputeRankSync(PluginConnection pluginConnection) {
		if (computationInProgress) {
			getLogger().info("Computing RDFRank already in progress");
		}
		startComputation();
		try {
			recomputeRank(pluginConnection.getStatements(), pluginConnection.getEntities());
		} finally {
			computationInProgress = false;
		}
	}

	private synchronized void recomputeIncrementalRankSync(PluginConnection pluginConnection) {
		if (computationInProgress) {
			getLogger().info("Computing RDFRank already in progress");
		}
		startComputation();
		try {
			recomputeIncrementalRank(pluginConnection.getStatements(), pluginConnection.getEntities());
		} catch (IOException t) {
			fail(t.getMessage());
		} finally {
			computationInProgress = false;
		}
	}

	/**
	 * Calls <b>recomputeRank()</b> in a separate thread.<br>
	 * Synchronized with <b>recomputeIncrementalRankAsync()</b>
	 *
	 * @throws PluginException in case of a {@link RuntimeException} while computing
	 */
	private synchronized void recomputeRankAsync(PluginConnection pluginConnection) {
		if (computationInProgress) {
			getLogger().info("Computing RDFRank already in progress");
			return;
		}

		ThreadsafePluginConnecton threadsafePluginConnecton = pluginConnection.getThreadsafeConnection();
		Statements threadSafeStatements = threadsafePluginConnecton.getStatements();
		Entities threadSafeEntities = threadsafePluginConnecton.getEntities();

		executor.submit(() -> {
			startComputation();
			try {
				recomputeRank(threadSafeStatements, threadSafeEntities);
			} catch (Throwable t) {
				error = t;
				fail(t.getMessage());
			} finally {
				threadsafePluginConnecton.close();
				computationInProgress = false;
			}
		});
	}

	private void recomputeRank(Statements statements, Entities entities) {
		getLogger().info("Computing RDF rank with epsilon=" + epsilon + " max-iterations=" + maxIterations);

		// create our plugin directory
		getDataDir().mkdirs();

		// prepare for reading of the whole repository
		BigDoubleArray ranks;
		try (GraphReader reader = getGraphReader(statements, entities)) {
			// load the graph and compute ranks
			computer = new RankComputer();
			computer.setMaxIterations(getMaxIterations());
			computer.setEpsilon(getEpsilon());
			computer.setDataDir(getDataDir());
			computer.setEntityBitSize(entities.getEntityIdSize());
			ranks = computer.compute(reader);
		}
		// If the computation is interrupted before completion the ranks will be an empty array
		if (interrupt || ranks == null) {
			computer = null;
			return;
		} else {
			// We want to persist the rank properties only if the computation has not been canceled.
			persistMinMaxRankProperties();
			computer = null;
		}

		// update fingerprint
		long nextFingerprint = 0;
		for (long id = 0; id < ranks.length(); id++) {
			nextFingerprint ^= Double.doubleToLongBits(id * (ranks.get(id) + 1));
		}
		setFingerprint(nextFingerprint);

		// write ranks to the storage file
		FileRankWriter writer = new FileRankWriter(getTempStorageFile());
		writer.write(ranks);

		// rename temporary file to the actual one
		File actualFile = new File(getStorageFile());
		File tempFile = new File(getTempStorageFile());

		actualFile.delete();
		tempFile.renameTo(actualFile);

		synchronized (rankReader) {
			rankReader.reload();
		}

		configuration.setComputedConfigCash(configuration.hashCode());
	}

	private void fail(String message) {
		getLogger().error(message);
		throw new PluginException(message);
	}

	/**
	 * Calls <b>recomputeIncrementalRank()</b> in a separate thread.<br>
	 * Synchronized with <b>recomputeRankAsync()</b>
	 * @throws PluginException in case of a {@link RuntimeException} while computing
	 */
	private synchronized void recomputeIncrementalRankAsync(PluginConnection pluginConnection) {
		if (computationInProgress) {
			getLogger().info("Computing RDFRank already in progress");
			return;
		}

		ThreadsafePluginConnecton threadsafePluginConnecton = pluginConnection.getThreadsafeConnection();
		Statements threadSafeStatements = threadsafePluginConnecton.getStatements();
		Entities threadSafeEntities = threadsafePluginConnecton.getEntities();

		executor.submit(() -> {
			startComputation();
			try {
				recomputeIncrementalRank(threadSafeStatements, threadSafeEntities);
			} catch (Throwable t) {
				error = t;
				fail(t.getMessage());
			} finally {
				threadsafePluginConnecton.close();
				computationInProgress = false;
			}
		});
	}

	/**
	 * Incrementally computes RDF rank for newly added nodes. ('newly added' means ones that were not in the
	 * repo during the last proper recomputeRank) Note: The thus calculated pseudo-ranks are appended to the
	 * ranks file, so that subsequent reload()s will consider them ranks proper, which they are not!
	 *
	 * @throws IOException
	 *             in case writing to the ranks file failed
	 */
	private void recomputeIncrementalRank(Statements statements, Entities entities) throws IOException {
		getLogger().info("Incrementally computing RDF rank");

		if (!new File(getStorageFile()).exists()) {
			fail("Can't incrementally compute rank if (proper) rank "
					+ "has not been computed for an (earlier) version of the repository");
		}

		long begId = rankReader.size(); // first entity to incrementally rank
		long endId = entities.size() + 1; // one-after the last entity to rank

		if (begId >= endId || endId <= lastRankedId() + 1) {
			getLogger().info("Nothing to be recomputed");
			// nothing to be recomputed
			return;
		}

		if (endId - begId > 100000000) { // 100M!
			fail("Incremental rank recomputation invoked on more than 100M new nodes");
		}

		int size = (int) (endId - begId);
		int[] nInboundLinks = new int[size];
		int[] nStableInboundLinks = new int[size];
		char[] stableInboundRank = new char[size];
		int maxLinks = 0, maxStableLinks = 0;
		double minOldRank, maxOldRank;
		try {
			minOldRank = configuration.getMinRank();
			maxOldRank = configuration.getMaxRank();
		} catch (NullPointerException e) {
			// We should only see this if we run incremental computation on a rank which has been
			// computed on a version of GDB prior to 8.5.
			throw new PluginException("No data for the existing rank boundaries can be found. Please perform a full computation.", e);
		}


		getLogger().info(String.format("Begin parsing of %d entities", endId - begId));
		// loop over nodes [begId, endId) to count:
		// - nInboundLinks
		// - nStableInboundLinks
		// - stableInboundRank
		for (long id = begId; id < endId; id++) {
			if (interrupt) {
				return;
			}
			long nLinks = 0;
			long nStableLinks = 0;
			double stableRank = .0;

			try (GraphReader gr = getGraphReader(statements, entities, id)) {
				while (gr.next()) {
					nLinks++;
					if (gr.from < begId) {
						nStableLinks++;
						double subjRank = getRank(gr.from);
						stableRank += subjRank;
					}
				}
			}

			if (maxLinks < nLinks) {
				maxLinks = (int) nLinks;
			}
			if (maxStableLinks < nStableLinks) {
				maxStableLinks = (int) nStableLinks;
			}

			if (nLinks > Integer.MAX_VALUE) {
				fail("Incremental rank recomputation invoked on too densely-connected set of new nodes");
			}

			int idx = (int) (id - begId);
			nInboundLinks[idx] = (int) nLinks;
			nStableInboundLinks[idx] = (int) nStableLinks;

			double norm = (nStableLinks != 0 ? stableRank / nStableLinks : 0);
			assert 0 <= norm && norm <= 1 : norm;
			if (norm < 0) {
				norm = 0;
			} else if (norm > 1) {
				norm = 1;
			}
			stableInboundRank[idx] = (char) (norm * Character.MAX_VALUE);
		}

		if (maxLinks == 0) {
			// no incoming links into any of the new nodes -- really nothing to compute
			persistLastRankedID(endId - 1);
			return;
		}

		double minNewRank = 1, maxNewRank = 0;

		getLogger().info("Begin rank computation");
		// First pass:
		// loop over nodes [begId, endId) to calculate incremental rank and write it to file.
		for (long id = begId; id < endId; id++) {
			int idx = (int) (id - begId);
			double simpleRank = (double) nInboundLinks[idx] / maxLinks;
			double c = (nInboundLinks[idx] != 0 ? (double) nStableInboundLinks[idx] / nInboundLinks[idx] : 0);
			assert (0 <= c && c <= 1) : c;
			double stableRank = (double) stableInboundRank[idx] / Character.MAX_VALUE;
			double finalRank = c * stableRank + (1 - c) * simpleRank;
			assert (0 <= finalRank && finalRank <= 1) : finalRank;
			if (finalRank < 0) {
				finalRank = minNewRank = 0;
			} else if (finalRank > 1) {
				finalRank = maxNewRank = 1;
			}
			if (finalRank < minNewRank) {
				minNewRank = finalRank;
			}
			if (finalRank > maxNewRank) {
				maxNewRank = finalRank;
			}

			stableInboundRank[idx] = (char) (finalRank * Character.MAX_VALUE);
			if (interrupt) {
				return;
			}
		}

		getLogger().info("Begin rank normalization");
		// Second pass:
		// What we do next is to distribute the new rank values in a more wide interval
		// *but* which is strictly confined within the old interval [minOldRank, maxOldRank]
		double minAdjRank = minNewRank < minOldRank ? minOldRank : 0.5 * (minNewRank + minOldRank);
		double maxAdjRank = maxNewRank > maxOldRank ? maxOldRank : 0.5 * (maxNewRank + maxOldRank);

		RandomAccessFile raf = new RandomAccessFile(getStorageFile(), "rw");
		long nextFingerprint = getFingerprint();
		try {
			setFilePointerToFileEnd(raf);

			if ((maxNewRank - minNewRank) > 0.0001) {
				// calculate the (static) redistribution quotients r & q:
				double q = (maxAdjRank - minAdjRank) / (maxNewRank - minNewRank);
				double r = minAdjRank - minNewRank * q;
				for (long id = begId; id < endId; id++) {
					int idx = (int) (id - begId);
					double finalRank = (double) stableInboundRank[idx] / Character.MAX_VALUE;
					assert (minNewRank <= finalRank && finalRank <= maxNewRank);
					double adjRank = finalRank * q + r;
					assert (minOldRank <= adjRank && adjRank <= maxOldRank);

					raf.writeInt((int) id);
					raf.writeDouble(adjRank);
					nextFingerprint ^= Double.doubleToLongBits(id * (adjRank + 1));
					// If we need to interrupt we leave the calculated to far ranks and store them
					if (interrupt) {
						break;
					}
				}
			} else {
				for (long id = begId; id < endId; id++) {
					assert (minOldRank <= maxAdjRank);

					raf.writeInt((int) id);
					raf.writeDouble(maxAdjRank);
					nextFingerprint ^= Double.doubleToLongBits(id * (maxAdjRank + 1));
					// If we need to interrupt we leave the calculated to far ranks and store them
					if (interrupt) {
						break;
					}
				}
			}
		} finally {
			raf.close();
		}
		setFingerprint(nextFingerprint);

		synchronized (rankReader) {
			rankReader.reload();
		}
		getLogger().info("Incremental rank computed");
	}

	/**
	 * Sets the file pointer to the end of the file
	 *
	 * @param raf
	 * @throws IOException if an I/O error occurs when working with the file
	 */
	private void setFilePointerToFileEnd(RandomAccessFile raf) throws IOException {
		raf.seek(raf.length());
	}

	private void exportRank(String path, Entities entities) throws IOException {
		try(BufferedWriter exportWriter = new BufferedWriter(new FileWriter(path))) {
			for (int id = 1; id < entities.size(); id++) {
				if (entities.getType(id) == Entities.Type.URI) {
					String formattedRank = getFormattedRank(id);
					exportWriter.write(entities.get(id) + " " + formattedRank + "\n");
				}
			}
		}

	}

	private String getStorageFile() {
		return getDataDir() + File.separator + STORAGE_FILE;
	}

	private String getTempStorageFile() {
		return getStorageFile() + TEMP_SUFFIX;
	}

	private String getStateFile() {
		return getDataDir() + File.separator + STATE_FILE;
	}

	public long getContextId() {
		return contextId;
	}

	@Override
	public double estimate(long subject, long predicate, long object, long context, PluginConnection pluginConnection,
						   RequestContext requestContext) {
		if (Utils.match(predicate, hasRDFRankID, hasRDFRankID_3, hasRDFRankID_4, hasRDFRankID_5)) {
			return subject == 0 ? pluginConnection.getEntities().size() : 1;
		}
		return 1;
	}

	/**
	 * Sets the control flags needed at a beginning of a computation task
	 */
	private void startComputation() {
		computationInProgress = true;
		error = null;
		interrupt = false;
	}

	/**
	 * Interrupt currently running RDFRank computation
	 */
	private void computationInterrupt(Entities entities) {
		if (getStatus(entities) != COMPUTING) {
			return;
		}
		interrupt = true;
		// If the computation is incremental we do not have computer instance
		if (computer != null) {
			computer.interrupt();
		}
	}

	private GraphReader getGraphReader(Statements statements, Entities entities) {
		return getGraphReader(statements, entities, 0);
	}

	/**
	 * Creates a {@link FilteredGraphReader} instance using the properties in the configuration object
	 *
	 * @param statements
	 * @param entities
	 * @param object 0 for wildcard
	 * @return
	 */
	private GraphReader getGraphReader(Statements statements, Entities entities, long object) {
		if (!configuration.getFilteringEnabled()) {
			return new OwlimGraphReader(statements, entities, object);
		}

		Collection<Value> c1 = configuration.getIncludedPredicates();
		Collection<Value> c2 = configuration.getIncludedGraphs();
		Collection<Value> c3 = configuration.getExcludedPredicates();
		Collection<Value> c4 = configuration.getExcludedGraphs();
		boolean includeExplicit = configuration.getIncludeExplicit();
		boolean includeImplicit = configuration.getIncludeImplicit();

		return new FilteredGraphReader(statements,
				entities,
				createPredicatesList(c1, entities),
				createGraphList(c2, entities),
				createPredicatesList(c3, entities),
				createGraphList(c4, entities),
				includeExplicit,
				includeImplicit,
				object);
	}

	/**
	 * Returns the current value of a filter list.<br>
	 *     Gets the list from the configuration and for each element in the list creates a statement in the format
	 *     <ul>
	 *         <li><b>subject</b> - the element</li>
	 *         <li><b>predicate</b> - the id the list</li>
	 *         <li><b>object</b> - literal <b>true</b></li>
	 *         <li><b>context</b> - default</li>
	 *     </ul>
	 *
 	 * @param predicate the id of the filter list
	 * @param entities
	 * @return StatementIterator of the list
	 */
	private StatementIterator getFilterList(long predicate, Entities entities) {
		RDFRankProperty property = getRDFListPropertyFromId(predicate);
		List<Long> items = configuration.getValueCollection(property)
				.stream()
				.map(value -> entities.put(value, Scope.REQUEST))
				.collect(Collectors.toList());
		long[][] resultsTriples = new long[items.size()][];
		for (int i = 0; i < items.size(); i++) {
			resultsTriples[i] = new long[] {
					items.get(i),
					predicate,
					entities.put(SimpleValueFactory.getInstance().createLiteral("true"), Scope.SYSTEM),
					0
			};
		}
		return StatementIterator.create(resultsTriples);
	}

	/**
	 * Adds a {@link Value} in a Filter list (include/exclude predicates/graphs) by resolving the value using the
	 * {@link Entities} instance and the id of the value in the entity pool
	 *
	 * @param valueId id of the value to be added
	 * @param listId id of the {@link RDFRankProperty} representing the list
	 * @param entities for resolving id -> value
	 */
	private void addToFilterList(long valueId, long listId, Entities entities) {
		RDFRankProperty property = getRDFListPropertyFromId(listId);
		Collection<Value> collection = configuration.getValueCollection(property);
		Value value = entities.get(valueId);

		if (collection.contains(value)) {
			getLogger().info(String.format("Value %s already present in the list %s", value.stringValue(), property.toString()));
			return;
		}

		collection.add(value);
		configuration.setValueCollection(property, collection);
		configuration.save();
	}

	/**
	 * Removes a {@link Value} from a Filter list (include/exclude predicates/graphs) by resolving the value using the
	 * {@link Entities} instance and the id of the value in the entity pool
	 *
	 * @param valueId id od the value to be added
	 * @param listId id of the {@link RDFRankProperty} representing the list
	 * @param entities for resolving id -> value
	 * @throws PluginException if the value we try to remove is not present
	 */
	private void removeFromFilterList(long valueId, long listId, Entities entities) {
		RDFRankProperty property = getRDFListPropertyFromId(listId);
		Collection<Value> collection = configuration.getValueCollection(property);
		if (!collection.remove(entities.get(valueId))) {
			throw new PluginException("Tried to remove Value which is not in the list: "
					+ entities.get(valueId) + ", " + property);
		}
		configuration.setValueCollection(property, collection);
		configuration.save();
	}

	/**
	 * Converts Collection of Value to Set of Long using the entities object to resolve Value to Long<br>
	 *     Used to create a set representing filter list with Predicate ids.
	 *
	 * @param collection
	 * @param entities
	 * @return
	 */
	private Set<Long> createPredicatesList(Collection<Value> collection, Entities entities) {
		return collection.stream().map(value -> entities.put(value, Scope.REQUEST)).collect(Collectors.toSet());
	}

	/**
	 * Converts Collection of Value to Set of Long using the entities object to resolve Value to Long<br>
	 *     Used to create a set representing filter list with Graphs ids. Applies any special mapping between
	 *     Graph
	 *
	 * @param collection
	 * @param entities
	 * @return
	 */
	private Set<Long> createGraphList(Collection<Value> collection, Entities entities) {
		Set<Long> result = new HashSet<>(collection.size());
		for (Value value : collection) {
			result.add(specialGraphsMapping.containsKey(value)
					? specialGraphsMapping.get(value)
					: entities.put(value, Scope.REQUEST));
		}
		return result;
	}

	/**
	 * If the provided id corresponds to one of the filter list properties (include/exclude predicate/graph)
	 * returns the corresponding {@link RDFRankProperty}.
	 *
	 * @param id
	 * @return
	 */
	private RDFRankProperty getRDFListPropertyFromId(long id) {
		Configuration.RDFRankProperty property = null;

		if (Utils.match(id, includedPredicates))
			property = INCLUDED_PREDICATES_PROPERTY;
		else if (Utils.match(id, includedGraphs))
			property = INCLUDED_GRAPHS_PROPERTY;
		else if (Utils.match(id, excludedPredicates))
			property = EXCLUDED_PREDICATES_PROPERTY;
		else if (Utils.match(id, excludedGraphs))
			property = EXCLUDED_GRAPHS_PROPERTY;

		return property;
	}

	/**
	 * Saves the current Min and Max rank in order to be used later in case of incremental computation.
	 */
	private void persistMinMaxRankProperties() {
		configuration.setMinRank(computer.getMinRank());
		configuration.setMaxRank(computer.getMaxRank());
		configuration.save();
	}

	/**
	 * Saves the new value of lastRankedId and persists it to the properties file
	 * @param id the id of the entity
	 */
	private void persistLastRankedID(long id) {
		configuration.setLastRankedId(id);
		configuration.save();
	}

	/**
	 * Returns the ID of the last entity which has been ranked or marked by the Incremental Rank as <br>
	 *     not needed to be ranked
	 * @return the id
	 */
	private Long lastRankedId() {
		return configuration.getLastRankedId();
	}

	/**
	 * Returns the current status of the plugin
	 * @param entities link to Entities used to calculate whether the rank is outdated
	 * @return
	 */
	private Status getStatus(Entities entities) {

		if (computationInProgress)
			return COMPUTING;

		if (error != null)
			return ERROR;

		if (interrupt)
			return CANCELED;

		if (rankReader.size() == 0)
			return EMPTY;

		if(configurationOutdated())
			return CONFIG_CHANGED;

		if (rankReader.size() < entities.size() + 1 && lastRankedId() < entities.size())
			return OUTDATED;

		return COMPUTED;
	}

	private boolean configurationOutdated() {
		// If filtering is disabled and last run is without filtering we do not have to check the filter config
		if (!configuration.getFilteringEnabled() && ((configuration.getLastConfigCash() & 1) == 0)) {
			return false;
		}

		return configuration.hashCode() != configuration.getLastConfigCash();
	}

	/**
	 * Available statuses of the plugin
	 */
	public enum Status {

		/**
		 * The ranks computation has been canceled
		 */
		CANCELED,

		/**
		 * The ranks are computed and up-to-date
		 */
		COMPUTED,

		/**
		 * A computing task is currently in progress
		 */
		COMPUTING,

		/**
		 * There are no calculated ranks
		 */
		EMPTY,

		/**
		 * Exception has been thrown during computation
		 */
		ERROR,

		/**
		 * The ranks are outdated and need computing
		 */
		OUTDATED,

		/**
		 * The filtering is enabled and it's configuration has been changed since the last full computation
		 */
		CONFIG_CHANGED
	}
}
