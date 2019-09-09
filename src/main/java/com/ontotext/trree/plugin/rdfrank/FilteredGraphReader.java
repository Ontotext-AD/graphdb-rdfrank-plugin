package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.StatementIterator;
import com.ontotext.trree.sdk.Statements;

import java.util.*;

public class FilteredGraphReader extends GraphReader {

	private static final Set<Long> EMPTY = Collections.emptySet();

	private Statements statements;
	private Entities entities;
	private Collection<StatementIterator> statementIterators;
	private Iterator<StatementIterator> statementIteratorsIterator;
	private StatementIterator currentStatementIterator = StatementIterator.EMPTY;

	private Set<Long> includedPredicates;
	private Set<Long> excludedPredicates;

	private Set<Long> includedGraphs;
	private Set<Long> excludedGraphs;

	private long object;

	private boolean includeImplicit;
	private boolean includeExplicit;

	private long size = 0;

	FilteredGraphReader(Statements statements, Entities entities, Set<Long> includedPredicates, Set<Long> includedGraphs, Set<Long> excludedPredicates, Set<Long> excludedGraphs, boolean includeExplicit, boolean includeImplicit) {
		this(statements, entities, includedPredicates, includedGraphs, excludedPredicates, excludedGraphs, includeExplicit, includeImplicit, 0);
	}

	FilteredGraphReader(Statements statements, Entities entities, Set<Long> includedPredicates, Set<Long> includedGraphs, Set<Long> excludedPredicates, Set<Long> excludedGraphs, boolean includeExplicit, boolean includeImplicit, long object) {
		this.statements = statements;
		this.entities = entities;
		this.includedPredicates = includedPredicates != null ? includedPredicates : EMPTY;
		this.excludedPredicates = excludedPredicates != null ? excludedPredicates : EMPTY;
		this.includedGraphs = includedGraphs != null ? includedGraphs : EMPTY;
		this.excludedGraphs = excludedGraphs != null ? excludedGraphs : EMPTY;
		this.includeImplicit = includeImplicit;
		this.includeExplicit = includeExplicit;
		this.object = object;
		reset();
	}

	@Override
	public long nodeCount() {
		return entities.size();
	}

	@Override
	public long size() {
		return size;
	}

	@Override
	public void reset() {
		statementIterators = new LinkedList<>();

		if (includedPredicates.isEmpty()) {
			includedPredicates.add(0L);
		}
		if (includedGraphs.isEmpty()) {
			includedGraphs.add(0L);
		}

		for (long predicate : includedPredicates) {
			for (long graph : includedGraphs) {
				size += statements.estimateSize(0, predicate, object, graph);
				statementIterators.add(statements.get(0, predicate, object, graph));
			}
		}

		statementIteratorsIterator = statementIterators.iterator();
	}

	@Override
	public boolean next() {
		if (currentStatementIterator.next()) {
			while (isExcluded()) {
				if (!currentStatementIterator.next()) {
					return false;
				}
			}
			to = currentStatementIterator.object;
			from = currentStatementIterator.subject;
			return true;
		} else {
			if (statementIteratorsIterator.hasNext()) {
				currentStatementIterator = statementIteratorsIterator.next();
				return next();
			}
		}
		return false;
	}

	@Override
	public void close() {
		for (StatementIterator iterator : statementIterators) {
			iterator.close();
		}
	}

	private boolean isExcluded() {
		return !includeImplicit && currentStatementIterator.isImplicit()
				|| excludedPredicates.contains(currentStatementIterator.predicate)
				|| excludedGraphs.contains(currentStatementIterator.context)
				|| !includeExplicit && currentStatementIterator.isExplicit();
	}

}
