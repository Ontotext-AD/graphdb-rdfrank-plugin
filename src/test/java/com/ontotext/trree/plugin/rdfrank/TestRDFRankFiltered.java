package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.sdk.PluginException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRDFRankFiltered extends TestRDFRankAbstract {
	@Override
	protected SailRepository createRepository() {
		return createRepositoryWithInference();
	}

	@Override
	public void setup() {
		super.setup();
		conn.add(RDFRank.FILTERING, RDFRank.SET_PARAM, vf.createLiteral("true"));
	}

	@Test
	public void testEmpty() {
		IRI context = vf.createIRI("http:context");
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2), context);
		computeRank();

		assertTrue(0 < getRank(valuesPool.get(2)));
	}

	@Test
	public void testIncludedPredicates() {
		IRI context = vf.createIRI("http:context");
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2), context);
		conn.add(valuesPool.get(0), valuesPool.get(2), valuesPool.get(3));
		conn.add(valuesPool.get(0), valuesPool.get(3), valuesPool.get(4));

		conn.add(valuesPool.get(2), RDFRank.INCLUDED_PREDICATES, vf.createLiteral("add"));

		computeRank();

		assertEquals(0.0, getRank(valuesPool.get(2)), 0.001);
		assertEquals(0.0, getRank(valuesPool.get(4)), 0.001);
		assertTrue(0 < getRank(valuesPool.get(3)));
	}

	@Test
	public void testExcludedPredicates() {
		IRI context = vf.createIRI("http:context");
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2), context);
		conn.add(valuesPool.get(0), valuesPool.get(3), valuesPool.get(4));
		conn.add(valuesPool.get(0), valuesPool.get(5), valuesPool.get(6));

		conn.add(valuesPool.get(3), RDFRank.EXCLUDED_PREDICATES, vf.createLiteral("add"));
		conn.add(valuesPool.get(5), RDFRank.EXCLUDED_PREDICATES, vf.createLiteral("add"));

		computeRank();

		assertTrue(0 < getRank(valuesPool.get(2)));
		assertEquals(0.0, getRank(valuesPool.get(4)), 0.001);
		assertEquals(0.0, getRank(valuesPool.get(4)), 0.001);
	}

	@Test
	public void testIncludeImplicit() {
		IRI context = vf.createIRI("http:context");
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2), context);
		conn.add(valuesPool.get(0), valuesPool.get(2), valuesPool.get(3));

		conn.add(valuesPool.get(1), RDFRank.EXCLUDED_PREDICATES, vf.createLiteral("add"));

		computeRank();
		assertTrue(0 < getRank(valuesPool.get(2)));

		conn.add(RDFRank.INCLUDE_IMPLICIT, RDFRank.SET_PARAM, vf.createLiteral("false"));

		computeRank();
		assertEquals(0.0, getRank(valuesPool.get(2)), 0.001);
	}

	@Test
	public void testExcludeDefault() {
		IRI sesameNil = vf.createIRI("http://www.openrdf.org/schema/sesame#nil");
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));

		conn.add(sesameNil, RDFRank.EXCLUDED_GRAPHS, vf.createLiteral("add"));

		computeRank();
		assertEquals(0.0, getRank(valuesPool.get(2)), 0.001);
	}

	@Test
	public void testIncludeDefault() {
		IRI sesameNil = vf.createIRI("http://www.openrdf.org/schema/sesame#nil");
		IRI context = vf.createIRI("http:context");

		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));
		conn.add(valuesPool.get(0), valuesPool.get(3), valuesPool.get(4), context);

		conn.add(sesameNil, RDFRank.INCLUDED_GRAPHS, vf.createLiteral("add"));

		computeRank();
		assertEquals(0.0, getRank(valuesPool.get(4)), 0.001);
	}

	@Test
	public void testDisabled() {
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));
		conn.add(valuesPool.get(1), RDFRank.EXCLUDED_PREDICATES, vf.createLiteral("add"));
		conn.add(RDFRank.FILTERING, RDFRank.SET_PARAM, vf.createLiteral("false"));
		computeRank();
		assertTrue(0 < getRank(valuesPool.get(2)));
	}

	@Test
	public void testGetList() {
		conn.add(valuesPool.get(1), RDFRank.EXCLUDED_GRAPHS, vf.createLiteral("add"));
		conn.add(valuesPool.get(2), RDFRank.EXCLUDED_GRAPHS, vf.createLiteral("add"));

		try (TupleQueryResult elements = conn.prepareTupleQuery("SELECT ?s WHERE {?s <" + RDFRank.EXCLUDED_GRAPHS + "> ?o .}").evaluate()) {
			expectValues(elements, valuesPool.get(1), valuesPool.get(2));
		}

		conn.add(valuesPool.get(1), RDFRank.EXCLUDED_GRAPHS, vf.createLiteral("remove"));

		try (TupleQueryResult elements = conn.prepareTupleQuery("SELECT ?s WHERE {?s <" + RDFRank.EXCLUDED_GRAPHS + "> ?o .}").evaluate()) {
			expectValues(elements, valuesPool.get(2));
		}
	}

	@Test
	public void testConfigChanged() {
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));
		computeRank();
		conn.add(valuesPool.get(1), RDFRank.EXCLUDED_GRAPHS, vf.createLiteral("add"));
		waitForStatus(RDFRankPlugin.Status.CONFIG_CHANGED);
	}

	@Test (expected = RuntimeException.class)
	public void testFilterReturningEmptyGraph() {
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));
		conn.add(valuesPool.get(3), RDFRank.INCLUDED_GRAPHS, vf.createLiteral("add"));
		conn.prepareBooleanQuery("ASK {_:b <" + RDFRank.COMPUTE + "> true .}").evaluate();
		assertEquals(0.0, getRank(valuesPool.get(2)), 0.001);
	}

	@Test (expected = RuntimeException.class)
	public void testIncludeBothImplicitAndExplicit() {
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));
		conn.add(RDFRank.INCLUDE_IMPLICIT, RDFRank.SET_PARAM, vf.createLiteral("false"));
		conn.add(RDFRank.INCLUDE_EXPLICIT, RDFRank.SET_PARAM, vf.createLiteral("false"));
		conn.prepareBooleanQuery("ASK {_:b <" + RDFRank.COMPUTE + "> true .}").evaluate();
	}

	private void expectValues(TupleQueryResult queryResult, Value... values) {
		List<Value> expected = new LinkedList<>();
		expected.addAll(Arrays.asList(values));
		while (queryResult.hasNext()) {
			assertTrue(expected.remove(queryResult.next().getBinding(queryResult.getBindingNames().get(0)).getValue()));
		}
		assertTrue(expected.isEmpty());
	}

}
