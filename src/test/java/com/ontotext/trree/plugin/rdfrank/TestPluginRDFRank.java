package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.test.TemporaryLocalFolder;
import com.ontotext.test.utils.TestUtil;
import com.ontotext.test.utils.Utils;
import com.ontotext.trree.OwlimSchemaRepository;
import com.ontotext.trree.entitypool.EntityPoolConnection;
import com.ontotext.trree.entitypool.EntityType;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestPluginRDFRank {
	private static final String NAMESPACE = RDFRank.NAMESPACE;

	private static final int DEFAULT_ITERATIONS = 10;
	private static final double DEFAULT_EPSILON = 0;

	private SailRepository repository;
	private SailConnection sailConn;
	private RepositoryConnection connection;
	private OwlimSchemaRepository schema;

	private boolean useUpdate;

	@Rule
	public TemporaryLocalFolder tmpFolder = new TemporaryLocalFolder();

	public TestPluginRDFRank(boolean useUpdate) {
		this.useUpdate = useUpdate;
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.<Object[]> asList(new Object[] { true }, new Object[] { false });
	}

	@Before
	public void setUp() throws RepositoryException, SailException {
		schema = new OwlimSchemaRepository();
		schema.setDataDir(new File("."));
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("storage-folder", tmpFolder.getRoot().getAbsolutePath());
		parameters.put("repository-type", "file-repository");
		parameters.put("ruleset", "empty");
		schema.setParameters(parameters);
		repository = new SailRepository(schema);
		repository.init();
		sailConn = schema.getConnection();
		connection = repository.getConnection();
		connection.clear();
		connection.commit();
	}

	@After
	public void tearDown() throws RepositoryException, SailException {
		connection.clear();
		connection.commit();
		connection.close();
		sailConn.close();
		repository.shutDown();
		schema.shutDown();
	}

	private void load(String fileName) {
		try {
			RDFFormat format = null;
			if (fileName.endsWith(".rdf") || fileName.endsWith(".owl")) {
				format = RDFFormat.RDFXML;
			}
			if (fileName.endsWith(".nt") || fileName.endsWith(".n3")) {
				format = RDFFormat.N3;
			}
			connection.add(TestPluginRDFRank.class.getResourceAsStream("/" + fileName), NAMESPACE, format);
			connection.commit();
			recomputeRank(DEFAULT_EPSILON, DEFAULT_ITERATIONS);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private boolean exec(String command) {
		command = (useUpdate ? "INSERT DATA" : "ASK") + " " + command;
		return useUpdate ? update(command) : ask(command);
	}

	private boolean ask(String query) {
		try {
			BooleanQuery askQuery = connection.prepareBooleanQuery(QueryLanguage.SPARQL, query, NAMESPACE);
			return askQuery.evaluate();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private boolean update(String query) {
		try {
			Update updateQuery = connection.prepareUpdate(QueryLanguage.SPARQL, query, NAMESPACE);
			updateQuery.execute();
			connection.commit();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private void recomputeRank(double epsilon, int iteratrions) {
		assertTrue(exec("{<" + RDFRank.EPSILON + "> <" + RDFRank.SET_PARAM + "> \"" + epsilon + "\"}"));
		assertTrue(exec("{<" + RDFRank.MAX_ITERATIONS + "> <" + RDFRank.SET_PARAM + "> \"" + iteratrions
				+ "\"}"));
		assertTrue(exec("{_:anon <" + RDFRank.COMPUTE + "> \"true\"}"));
		waitForStatus(RDFRankPlugin.Status.COMPUTED);
	}

	private void check(String fileName) {
		HashMap<String, String> ranks = new HashMap<String, String>();
		// parse the ranks file
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(TestPluginRDFRank.class.getResourceAsStream("/" + fileName)));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\\s+");
				/**
				 * Ignore bnodes
				 */
				if (!parts[0].startsWith("_:")) {
					ranks.put(parts[0], parts[1]);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		EntityPoolConnection entities = schema.getEntities().getConnection();
		try { 
			// for each URI in the file check that we have it in the repository
			try {
				for (String uri : ranks.keySet()) {
					String rankString = ranks.get(uri);
					if (uri.startsWith("_:")) {
						continue; // can't query about blank nodes
					}
					TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?r WHERE {<"
							+ uri + "> <" + RDFRank.HAS_RDF_RANK + "> ?r}");
					try (TupleQueryResult result = query.evaluate()) {
						int count = 0;
						while (result.hasNext()) {
							BindingSet bs = result.next();
							Value rankValue = bs.getValue("r");
							assertTrue(rankValue instanceof Literal);
							assertTrue(((Literal) rankValue).getDatatype() instanceof IRI);
							assertTrue(((Literal) rankValue).getDatatype().stringValue()
									.equals("http://www.w3.org/2001/XMLSchema#float"));
							assertTrue(rankValue.stringValue().equals(rankString));
							count++;
						}
						assertEquals(1, count);
					}
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
	
			// for each RDF rank in the repository make sure that it is present in the file
			try {
				TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?u ?r WHERE {?u <"
						+ RDFRank.HAS_RDF_RANK + "> ?r}");
				try (TupleQueryResult result = query.evaluate()) {
					while (result.hasNext()) {
						BindingSet bs = result.next();
						Value uriValue = bs.getValue("u");
						if (uriValue instanceof BNode) {
							continue;
						}
						Value rankValue = bs.getValue("r");
						String uri = uriValue.toString();
						assertTrue(uriValue instanceof Resource);
						assertTrue(rankValue instanceof Literal);
						assertEquals("URI = " + uri, ranks.get(uri), rankValue.stringValue());
					}
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			// count the non-literal entities
			long numberOfEntities = entities.size();

			// count the non-literal entities
			for (long id = 1; id <= entities.size(); id++) {
				final EntityType entityType = entities.getEntityType(id);
				if (entityType == EntityType.GENERIC_LITERAL || entityType == EntityType.BNODE) {
					numberOfEntities--;
				}

			}
			assertEquals(ranks.keySet().size(), numberOfEntities);
		} finally {
			entities.close();
		}
	}

	@Test
	public void test1() {
		load("TestPluginRDFRank1.nt");
		check("TestPluginRDFRank1.out");
	}

	@Test
	public void test2() {
		load("TestPluginRDFRank2.rdf");
		check("TestPluginRDFRank2.out");
	}

	@Test
	public void test3() {
		load("TestPluginRDFRank1.nt");
		Iterator<BindingSet> iter = Utils.evaluateQuery(sailConn,
				"PREFIX ns: <http://www.ontotext.com/owlim/rdfrank/test#> "
						+ "PREFIX rank: <http://www.ontotext.com/owlim/RDFRank#> "
						+ "SELECT ?o ?r WHERE {ns:a ?p ?o . ?o rank:hasRDFRank ?r}", QueryLanguage.SPARQL,
				true);
		while (iter.hasNext()) {
			BindingSet bs = iter.next();
			if (bs.getValue("o").stringValue().equals("http://www.ontotext.com/owlim/rdfrank/test#b")) {
				assertTrue(bs.getValue("r").stringValue().equals("0.26"));
			} else if (bs.getValue("o").stringValue().equals("http://www.ontotext.com/owlim/rdfrank/test#c")) {
				assertTrue(bs.getValue("r").stringValue().equals("1.00"));
			} else {
				fail("Unexpected binding: " + bs.getValue("o"));
			}
		}
	}

	@Test
	public void testLeaks() {
		load("TestPluginRDFRank2.rdf");
		for (int idx = 0; idx < 35; idx++) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			recomputeRank(DEFAULT_EPSILON, DEFAULT_ITERATIONS);
		}

	}

	private void checkNonDefaultPrecision(int precision, String expectedB, String expectedC) {
		Iterator<BindingSet> iter = Utils.evaluateQuery(sailConn,
				"PREFIX ns: <http://www.ontotext.com/owlim/rdfrank/test#> "
						+ "PREFIX rank: <http://www.ontotext.com/owlim/RDFRank#> "
						+ "SELECT ?o ?r WHERE {ns:a ?p ?o . ?o rank:hasRDFRank"+precision+" ?r}", QueryLanguage.SPARQL,
				true);
		while (iter.hasNext()) {
			BindingSet bs = iter.next();
			if (bs.getValue("o").stringValue().equals("http://www.ontotext.com/owlim/rdfrank/test#b")) {
				assertEquals(expectedB, bs.getValue("r").stringValue());
			} else if (bs.getValue("o").stringValue().equals("http://www.ontotext.com/owlim/rdfrank/test#c")) {
				assertEquals(expectedC, bs.getValue("r").stringValue());
			} else {
				fail("Unexpected binding: " + bs.getValue("o"));
			}
		}
	}
	@Test
	public void testNonDefaultPrecision3() {
		load("TestPluginRDFRank1.nt");
		checkNonDefaultPrecision(3, "0.260", "1.000");
	}
	@Test
	public void testNonDefaultPrecision4() {
		load("TestPluginRDFRank1.nt");
		checkNonDefaultPrecision(4, "0.2597", "1.0000");
	}
	@Test
	public void testNonDefaultPrecision5() {
		load("TestPluginRDFRank1.nt");
		checkNonDefaultPrecision(5, "0.25974", "1.00000");
	}

	private void waitForStatus(RDFRankPlugin.Status status) {
		int counter = 10;
		String currentStatus = null;
		while (counter-- > 0) {
			Iterator<BindingSet> query = Utils.evaluateQuery(sailConn, "SELECT ?o WHERE {_:b <" + RDFRank.STATUS + "> ?o}", QueryLanguage.SPARQL, true);
			if (!query.hasNext())
				break;
			currentStatus = query.next().getBinding("o").getValue().stringValue();

			if (currentStatus.equals(status.toString())) {
				break;
			}

			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		assertEquals("Plugin status", status.toString(), currentStatus);

	}
}
