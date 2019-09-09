package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.StatementIterator;
import com.ontotext.trree.sdk.Statements;

class OwlimGraphReader extends GraphReader {
	private Statements statements;
	private Entities entities;
	private StatementIterator iterator;
	private long object;

	OwlimGraphReader(Statements statements, Entities entities, long object) {
		this.statements = statements;
		this.entities = entities;
		this.object = object;
		reset();
	}

	@Override
	public boolean next() {
		boolean result = iterator.next();
		if (result) {
			from = iterator.subject;
			to = iterator.object;
		}
		return result;
	}

	@Override
	public void reset() {
		iterator = statements.get(0, 0, object, 0);
	}

	@Override
	public long size() {
		return statements.estimateSize(0, 0, object, 0);
	}

	@Override
	public long nodeCount() {
		return entities.size();
	}

	@Override
	public void close() {
		iterator.close();
	}
}
