package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.StatementIdIterator;
import com.ontotext.trree.big.collections.PairCollection;
import com.ontotext.trree.big.collections.PairCollection.PairConnection;
import com.ontotext.trree.transactions.TransactionException;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

public class TableStorage {
	private final long rows;
	private final long cols;

	private final PairCollection collection;
	private PairConnection connection;

	public abstract static class Iterator {
		public abstract boolean hasNext();

		public abstract long next();

		public abstract long size();
	}

	public TableStorage(String pathPrefix, long x, long y, int valuesBitSize) {
		rows = x;
		cols = y;
		File main_file = new File(pathPrefix);
		File index_file = new File(pathPrefix+".index");
		main_file.delete(); index_file.delete();
		collection = new PairCollection(main_file, 1000, valuesBitSize);
		try {
			collection.initialize();
		} catch (TransactionException e) {
			LoggerFactory.getLogger(this.getClass()).error("Failed initializing collection", e);
			return;
		}
		connection = collection.getConnection();
		try {
			connection.beginTransaction();
		} catch (TransactionException e) {
			LoggerFactory.getLogger(getClass()).error("Failed starting transaction", e);
			connection = null;
		}
	}

	public void add(long x, long y) {
		if (0 > x || x >= rows || 0 > y || y >= cols) {
			throw new IndexOutOfBoundsException();
		}
		if (connection == null)
			throw new IllegalStateException("Storage not initialized");
		connection.add(x, y);
	}

	public Iterator rowIterator(final long row) {
		return new Iterator() {
			private long[] values = new long[10];
			private int size, curr;
			{
				try (StatementIdIterator iter = connection.get(row, Long.MIN_VALUE, row, Long.MAX_VALUE)) {
					while (iter.hasNext()) {
						if (size >= values.length) {
							values = Arrays.copyOf(values, values.length * 2);
						}
						values[size++] = iter.pred;
						iter.next();
					}
				}
			}

			@Override
			public boolean hasNext() {
				return curr < size;
			}

			@Override
			public long next() {
				return values[curr++];
			}

			@Override
			public long size() {
				return size;
			}
		};
	}

	public void shutDown() {
		try {
			if (connection != null) {
				connection.commit();
			}
		} catch (TransactionException ex) {
			LoggerFactory.getLogger(this.getClass()).error("Failed committing collection", ex);
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
		collection.shutdown();
	}
}
