package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.test.TemporaryLocalFolder;
import com.ontotext.trree.OwlimConnection;
import com.ontotext.trree.OwlimSchemaRepository;
import com.ontotext.trree.StatementIdIterator;
import com.ontotext.trree.big.AVLRepositoryConnection;
import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.Statements;
import com.ontotext.trree.sdk.impl.PluginManager;
import com.ontotext.trree.sdk.impl.StatementsImpl;
import com.ontotext.trree.transactions.TransactionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.*;

public class TestFilteredGraphReader {

	private static final Set<Long> EMPTY = new HashSet<>(0);


	private OwlimSchemaRepository owlimSchemaRepository;
	private OwlimConnection owlimConnection;
	private AVLRepositoryConnection conn;
	private GraphReader graphReader;

	@Rule
	public TemporaryLocalFolder tmpFolder = new TemporaryLocalFolder();

	@Before
	public void setup() {
		owlimSchemaRepository = createRepository();
		owlimConnection = owlimSchemaRepository.getOwlimConnection();
		conn = (AVLRepositoryConnection) owlimConnection.getRepositoryConnection();
		createTestData();
	}

	@After
	public void cleanup() {
		conn.close();
		owlimConnection.close();
		owlimSchemaRepository.shutDown();
		tmpFolder.delete();
	}

	@Test
	public void testEmptySets() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, EMPTY, EMPTY, EMPTY, true, true, 0);
		expect(1l, 2l, 3l, 4l);
	}

	@Test
	public void testPredicateInclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, getSet(1l), EMPTY, EMPTY, EMPTY, true, true, 0);
		expect(1l, 3l);
	}

	@Test
	public void testGraphInclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, getSet(1l), EMPTY, EMPTY, true, true, 0);
		expect(2l);
	}

	@Test
	public void testPredicateExclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, EMPTY, getSet(1l), EMPTY, true, true, 0);
		expect(2l, 4l);
	}

	@Test
	public void testGraphExclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, EMPTY, EMPTY, getSet(1l), true, true, 0);
		expect(1l, 3l, 4l);
	}

	@Test
	public void testImplicitExclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, EMPTY, EMPTY, EMPTY, true, false, 0);
		expect(1l, 2l);
	}

	@Test
	public void testExplicitExclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, EMPTY, EMPTY, EMPTY, false, true, 0);
		expect(3l, 4l);
	}

	@Test
	public void testDefaultGraphInclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, getSet(-3l), EMPTY, EMPTY, true, true, 0);
		expect(1l);
	}

	@Test
	public void testDefaultGraphExclude() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, EMPTY, EMPTY, getSet(-3l), true, true, 0);
		expect(2l, 3l, 4l);
	}

	@Test
	public void testDuplicates() {
		// Here we do not want entity 2 to be returned twice - once because it has predicate 2 and once because it in graph 1
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, getSet(2l), getSet(1l), EMPTY, EMPTY, true, true, 0);
		expect(2l);
	}

	@Test
	public void testSpecifiedObject() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, EMPTY, EMPTY, EMPTY, EMPTY, true, true, 3);
		expect(3l);
	}

	@Test
	public void testEmpty() {
		graphReader = createFilteredGraphReader(new StatementsImpl(Mockito.mock(PluginManager.class), conn), null, getSet(1l), getSet(1l), EMPTY, EMPTY, true, true, 0);
		expect();
	}
	
	private GraphReader createFilteredGraphReader(Statements statements, Entities entities, Set<Long> includedPredicates, Set<Long> includedGraphs, Set<Long> excludedPredicates, Set<Long> excludedGraphs, boolean includeExplicit, boolean includeImplicit, long object) {
		FilteredGraphReader graphReader = new FilteredGraphReader(statements, entities, includedPredicates, includedGraphs, excludedPredicates, excludedGraphs, includeExplicit, includeImplicit, object);
		graphReader.reset();
		return graphReader;
	}

	private void createTestData() {
		try {
			conn.beginTransaction();
			owlimSchemaRepository.transactionStarted(1);

			conn.putStatement(1, 1, 1, 0, StatementIdIterator.EXPLICIT_STATEMENT_STATUS);
			conn.putStatement(2, 2, 2, 1, StatementIdIterator.EXPLICIT_STATEMENT_STATUS);

			conn.putStatement(3, 1, 3, 0, StatementIdIterator.INFERRED_STATEMENT_STATUS);
			conn.putStatement(4, 2, 4, 0, StatementIdIterator.INFERRED_STATEMENT_STATUS);

			conn.commit();
			owlimSchemaRepository.transactionCommit(1);
			owlimSchemaRepository.transactionCompleted(1);
		} catch (TransactionException e) {
			e.printStackTrace();
		}
	}

	private OwlimSchemaRepository createRepository() {
		OwlimSchemaRepository sail = new OwlimSchemaRepository();
		Map<String, String> parameters = new HashMap<>();
		parameters.put("storage-folder", tmpFolder.getRoot().getAbsolutePath());
		parameters.put("ruleset", "empty");
		sail.setParameters(parameters);
		sail.init();

		return sail;
	}

	private void expect(Long... fromNodes) {
		List<Long> expected = new LinkedList<>();
		expected.addAll(Arrays.asList(fromNodes));
		while (graphReader.next()) {
			assertTrue(expected.remove(graphReader.from));
		}
		assertTrue(expected.isEmpty());
	}

	private Set<Long> getSet(Long... elements) {
		Set<Long> includePredicate = new HashSet<>(1);
		includePredicate.addAll(Arrays.asList(elements));
		return includePredicate;
	}
}
