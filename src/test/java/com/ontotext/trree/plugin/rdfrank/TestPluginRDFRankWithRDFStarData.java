package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.test.utils.Utils;
import com.ontotext.trree.entitypool.EntityPoolConnection;
import com.ontotext.trree.entitypool.EntityType;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestPluginRDFRankWithRDFStarData extends TestPluginRDFRankBase {
	private SimpleValueFactory VF = SimpleValueFactory.getInstance();

	public TestPluginRDFRankWithRDFStarData(boolean useUpdate) {
		super(useUpdate);
	}

	@Before
	public void setUp() throws RepositoryException, SailException {
		super.setUp();
		load("TestPluginRDFRankRDFStarData.ttls");
	}

	protected void check(String fileName) {
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
						case 2: {
							ranks.put(VF.createIRI(parts[0]), parts[1]);
							break;
						}
						case 4: {
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
}
