package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.test.TemporaryLocalFolder;
import com.ontotext.test.utils.Utils;
import com.ontotext.trree.OwlimSchemaRepository;
import com.ontotext.trree.entitypool.EntityPoolConnection;
import com.ontotext.trree.entitypool.EntityType;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestPluginRDFRankWithRDFStarData {
	private SimpleValueFactory VF = SimpleValueFactory.getInstance();

	protected static final String NAMESPACE = RDFRank.NAMESPACE;

	protected static final int DEFAULT_ITERATIONS = 10;
	protected static final double DEFAULT_EPSILON = 0;

	private SailRepository repository;
	protected SailConnection sailConn;
	protected RepositoryConnection connection;
	protected OwlimSchemaRepository schema;

	protected boolean useUpdate;

	@Rule
	public TemporaryLocalFolder tmpFolder = new TemporaryLocalFolder();

	public TestPluginRDFRankWithRDFStarData(boolean useUpdate) {
		this.useUpdate = useUpdate;
	}

	@Parameterized.Parameters
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
		load("TestPluginRDFRankRDFStarData.ttls");
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

	protected void load(String fileName) {
		try {
			connection.add(TestPluginRDFRank.class.getResourceAsStream("/" + fileName), NAMESPACE, RDFFormat.TURTLESTAR);
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

	protected void recomputeRank(double epsilon, int iteratrions) {
		assertTrue(exec("{<" + RDFRank.EPSILON + "> <" + RDFRank.SET_PARAM + "> \"" + epsilon + "\"}"));
		assertTrue(exec("{<" + RDFRank.MAX_ITERATIONS + "> <" + RDFRank.SET_PARAM + "> \"" + iteratrions
				+ "\"}"));
		assertTrue(exec("{_:anon <" + RDFRank.COMPUTE + "> \"true\"}"));
		waitForStatus(RDFRankPlugin.Status.COMPUTED);
	}

	private void check(String fileName) {
		HashMap<Value, String> ranks = new HashMap<>();
		// parse the ranks file
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(TestPluginRDFRankWithRDFStarData.class.getResourceAsStream("/" + fileName)));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\\s+");
				/**
				 * Ignore bnodes
				 */
				if (!parts[0].startsWith("_:")) {
					switch (parts.length) {
						case  2 : {
							ranks.put(VF.createIRI(parts[0]), parts[1]);
							break;
						}
						case  4 : {
							ranks.put(VF.createTriple(VF.createIRI(parts[0]), VF.createIRI(parts[1]), VF.createIRI(parts[2])), parts[3]);
							// Note that entities that are part of embedded triple are also in the entity pool
							// And because they are part just of the embedded triple have rank 0.0
							ranks.put(VF.createIRI(parts[0]), "0.00");
							ranks.put(VF.createIRI(parts[1]), "0.00");
							ranks.put(VF.createIRI(parts[2]), "0.00");
							break;
						}
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		EntityPoolConnection entities = schema.getEntities().getConnection();
		try { 
			// for each URI in the file, check that we have it in the repository
			// for each Triple in the file, check that we have its components in the repository
			try {
				for (Value uri : ranks.keySet()) {
					String rankString = ranks.get(uri);
					if (uri instanceof BNode) {
						continue; // can't query about blank nodes
					}
					String queryVar;
					if (uri instanceof Triple) {
						queryVar = "<<<" + ((Triple) uri).getSubject() + "> <" + ((Triple) uri).getPredicate() + "> <" + ((Triple) uri).getObject() + ">>>";
					} else {
						queryVar = "<" + uri.stringValue() + ">";
					}
					TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?r WHERE {"
							+ queryVar + " <" + RDFRank.HAS_RDF_RANK + "> ?r}");
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
						assertTrue(uriValue instanceof Resource);
						assertTrue(rankValue instanceof Literal);
						assertEquals(((uriValue instanceof Triple ? "TRIPLE = " : "URI = ") + uriValue.stringValue()),
								ranks.get(uriValue), rankValue.stringValue());
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
	public void testBasicRDFRank() {
		check("TestPluginRDFRankRDFStarData.out");
	}

	@Test
	public void testRDFRankWithUpdateOfRDFStarData() {
		Iterator<BindingSet> iter = Utils.evaluateQuery(sailConn,
				"PREFIX ns: <http://www.ontotext.com/owlim/rdfrank/test#> "
						+ "PREFIX rank: <http://www.ontotext.com/owlim/RDFRank#> "
						+ "SELECT ?o ?r WHERE {<<ns:a <urn:1> <urn:2>>> ?p ?o . ?o rank:hasRDFRank ?r}", QueryLanguage.SPARQL,
				true);
		while (iter.hasNext()) {
			BindingSet bs = iter.next();
			if (bs.getValue("o").stringValue().equals("<<http://www.ontotext.com/owlim/rdfrank/test#b urn:5 urn:8>>")) {
				assertTrue(bs.getValue("r").stringValue().equals("0.26"));
			} else if (bs.getValue("o").stringValue().equals("<<http://www.ontotext.com/owlim/rdfrank/test#c urn:10 urn:15>>")) {
				assertTrue(bs.getValue("r").stringValue().equals("1.00"));
			} else {
				fail("Unexpected binding: " + bs.getValue("o"));
			}
		}
	}

	private void checkNonDefaultPrecision(int precision, String expectedB, String expectedC) {
		Iterator<BindingSet> iter = Utils.evaluateQuery(sailConn,
				"PREFIX ns: <http://www.ontotext.com/owlim/rdfrank/test#> "
						+ "PREFIX rank: <http://www.ontotext.com/owlim/RDFRank#> "
						+ "SELECT ?o ?r WHERE {<<ns:a <urn:1> <urn:2>>> ?p ?o . ?o rank:hasRDFRank"+precision+" ?r}", QueryLanguage.SPARQL,
				true);
		while (iter.hasNext()) {
			BindingSet bs = iter.next();
			if (bs.getValue("o").stringValue().equals("<<http://www.ontotext.com/owlim/rdfrank/test#b urn:5 urn:8>>")) {
				assertEquals(expectedB, bs.getValue("r").stringValue());
			} else if (bs.getValue("o").stringValue().equals("<<http://www.ontotext.com/owlim/rdfrank/test#c urn:10 urn:15>>")) {
				assertEquals(expectedC, bs.getValue("r").stringValue());
			} else {
				fail("Unexpected binding: " + bs.getValue("o"));
			}
		}
	}
	@Test
	public void testNonDefaultPrecision3() {
		checkNonDefaultPrecision(3, "0.260", "1.000");
	}
	@Test
	public void testNonDefaultPrecision4() {
		checkNonDefaultPrecision(4, "0.2597", "1.0000");
	}
	@Test
	public void testNonDefaultPrecision5() {
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
