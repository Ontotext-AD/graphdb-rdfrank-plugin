package com.ontotext.trree.plugin.rdfrank;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

class RDFRank {
	static final String NAMESPACE = "http://www.ontotext.com/owlim/RDFRank#";

	static final IRI HAS_RDF_RANK = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "hasRDFRank");
	static final IRI SET_PARAM = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "setParam");
	static final IRI MAX_ITERATIONS = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "maxIterations");
	static final IRI EPSILON = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "epsilon");
	static final IRI COMPUTE = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "compute");
	static final IRI COMPUTE_INCREMENTAL = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "computeIncremental");
	static final IRI COMPUTE_ASYNC = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "computeAsync");
	static final IRI COMPUTE_ASYNC_INCREMENTAL = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "computeIncrementalAsync");
	static final IRI EXPORT = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "export");
	static final IRI CONTEXT = SimpleValueFactory.getInstance().createIRI(NAMESPACE);
	static final IRI HAS_RDF_RANK_3 = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "hasRDFRank3");
	static final IRI HAS_RDF_RANK_4 = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "hasRDFRank4");
	static final IRI HAS_RDF_RANK_5 = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "hasRDFRank5");
	static final IRI STATUS = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "status");
	static final IRI PRESENT = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "present");
	static final IRI INTERRUPT = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "interrupt");

	static final IRI FILTERING = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "filtering");
	static final IRI INCLUDED_PREDICATES = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "includedPredicates");
	static final IRI INCLUDED_GRAPHS = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "includedGraphs");
	static final IRI EXCLUDED_PREDICATES = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "excludedPredicates");
	static final IRI EXCLUDED_GRAPHS = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "excludedGraphs");
	static final IRI INCLUDE_EXPLICIT = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "includeExplicit");
	static final IRI INCLUDE_IMPLICIT = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "includeImplicit");
}
