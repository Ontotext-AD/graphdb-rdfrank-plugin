package com.ontotext.trree.plugin.rdfrank;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.TupleQueryResult;

import static org.junit.Assert.assertTrue;

public class TestRDFRankComputationAsync extends TestRDFRankComputation {

	public TestRDFRankComputationAsync(boolean filterEnabled) {
		super(filterEnabled);
	}


	@Override
	protected IRI getComputePredicate() {
		return RDFRank.COMPUTE_ASYNC;
	}

	@Override
	protected IRI getComputeIncrementalPredicate() {
		return RDFRank.COMPUTE_ASYNC_INCREMENTAL;
	}

	@Override
	protected void waitForStatus(RDFRankPlugin.Status status) {
		int counter = 20;
		String currentStatus = null;
		while (counter-- > 0) {
			try (TupleQueryResult query = conn.prepareTupleQuery("SELECT ?o WHERE {_:b <" + RDFRank.STATUS + "> ?o}").evaluate()) {
				currentStatus = query.next().getBinding("o").getValue().stringValue();

				if (currentStatus.equals(status.toString()) || currentStatus.startsWith(status.toString())) {
					break;
				}

				try {
					Thread.sleep(300);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		assertTrue("Plugin status", currentStatus.startsWith(status.toString()));
	}
}
