package com.ontotext.trree.plugin.rdfrank;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public abstract class TestRDFRankComputation extends TestRDFRankAbstract {

	private boolean filterEnabled;

	public TestRDFRankComputation(boolean filterEnabled) {
		this.filterEnabled = filterEnabled;
	}

	@Parameterized.Parameters
	public static List<Object[]> getParameters() {
		return Arrays.<Object[]> asList(new Object[] { true }, new Object[] { false });
	}

	@Test
	public void testFullComputation() {
		conn.add(RDFRank.FILTERING, RDFRank.SET_PARAM, vf.createLiteral(filterEnabled));

		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));
		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(3));
		conn.add(valuesPool.get(1), valuesPool.get(2), valuesPool.get(3));

		waitForStatus(RDFRankPlugin.Status.EMPTY);
		computeRank();

		assertEquals(0.0, getRank(valuesPool.get(0)), 0.001);
		assertEquals(0.0, getRank(valuesPool.get(1)), 0.001);
		assertEquals(0.33, getRank(valuesPool.get(2)), 0.001);
		assertEquals(1.0, getRank(valuesPool.get(3)), 0.001);

	}

	@Test
	public void testIncrementalComputation() {
		conn.add(RDFRank.FILTERING, RDFRank.SET_PARAM, vf.createLiteral(filterEnabled));

		conn.add(valuesPool.get(0), valuesPool.get(1), valuesPool.get(2));

		computeRank();

		conn.add(valuesPool.get(1), valuesPool.get(2), valuesPool.get(3));

		waitForStatus(RDFRankPlugin.Status.OUTDATED);
		assertEquals(0, getRank(valuesPool.get(3)), 0.001);

		computeIncrementalRank();

		waitForStatus(RDFRankPlugin.Status.COMPUTED);
		assertEquals(0.5, getRank(valuesPool.get(3)), 0.001);

	}

}
