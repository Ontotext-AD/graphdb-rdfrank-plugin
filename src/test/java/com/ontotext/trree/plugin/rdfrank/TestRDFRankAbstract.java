package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.test.TemporaryLocalFolder;
import com.ontotext.trree.OwlimSchemaRepository;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class TestRDFRankAbstract {
	static ValueFactory vf;
	@Rule
	public TemporaryLocalFolder tmpFolder = new TemporaryLocalFolder();
	RepositoryConnection conn;
	List<IRI> valuesPool = new LinkedList<>();
	private SailRepository repo;

	@Before
	public void setup() {
		repo = createRepository();
		conn = repo.getConnection();
		vf = repo.getValueFactory();
		for (int i = 0; i < 10; i++) {
			valuesPool.add(vf.createIRI("http:entity" + i));
		}
	}

	@After
	public void clean() {
		conn.close();
		repo.shutDown();
		tmpFolder.delete();
	}

	void waitForStatus(RDFRankPlugin.Status status) {
		// ignore as now computation is syched by default
	}

	double getRank(IRI resource) {
		try (RepositoryResult<Statement> statements = conn.getStatements(resource, RDFRank.HAS_RDF_RANK, null)) {
			if (statements.hasNext()) {
				return Double.valueOf(statements.next().getObject().stringValue());
			} else {
				return -1;
			}
		}
	}


	void computeRank() {
		conn.prepareBooleanQuery("ASK {_:b <" + getComputePredicate() + "> true .}").evaluate();

		waitForStatus(RDFRankPlugin.Status.COMPUTED);
	}

	void computeIncrementalRank() {
		conn.prepareBooleanQuery("ASK {_:b <" + getComputeIncrementalPredicate() + "> true .}").evaluate();

		waitForStatus(RDFRankPlugin.Status.COMPUTED);
	}

	IRI getComputePredicate() {
		return RDFRank.COMPUTE;
	}

	IRI getComputeIncrementalPredicate() {
		return RDFRank.COMPUTE_INCREMENTAL;
	}

	SailRepository createRepository() {
		return createRepositoryWithoutInference();
	}

	private SailRepository createRepositoryWithoutInference() {
		OwlimSchemaRepository sail = new OwlimSchemaRepository();
		Map<String, String> parameters = new HashMap<>();
		parameters.put("storage-folder", tmpFolder.getRoot().getAbsolutePath());
		parameters.put("ruleset", "empty");
		sail.setParameters(parameters);
		sail.initialize();

		return new SailRepository(sail);
	}

	SailRepository createRepositoryWithInference() {
		OwlimSchemaRepository sail = new OwlimSchemaRepository();
		Map<String, String> parameters = new HashMap<>();
		parameters.put("storage-folder", tmpFolder.getRoot().getAbsolutePath());
		sail.setParameters(parameters);
		sail.initialize();

		return new SailRepository(sail);
	}
}
