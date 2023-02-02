package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.sdk.RDFRankProvider;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * A class providing reading access to the rank storage binary files. It supports reading of rank values and
 * the rank thresholds. The class methods are not thread-safe (except for {@link #getThresholds()}) so the
 * respective synchronization care should be taken when using the class.
 */
class FileRankReader {
	private static final int SIZE_OF_RECORD = 12;
	private static final int PAGE_SIZE = 1000;
	private static final int MAX_PAGES = 1000;

	private String file;

	private long size; // number of rank records present in the file
	private int headerSize; // header size in number of bytes
	private int availablePages;

	private double[] thresholds;
	private Page[] pages;

	private static class Page {
		private boolean isInitialized;
		private double[] ranks = new double[PAGE_SIZE];
		private int hits = 0;
	}

	FileRankReader(String rankFile) {
		file = rankFile;
		reload();
	}

	/**
	 * Reloads the ranks file contents
	 */
	void reload() {
		headerSize = 0;

		// read storage file header
		DataInputStream stream;
		try {
			stream = new DataInputStream(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			return;
		}
		try {
			int magic = stream.readInt();
			headerSize += 4;
			if (magic != RankUtils.MAGIC) {
				throw new RuntimeException(
						"Invalid PageRank data detected. Please recompute PageRank values.");
			}
			int version = stream.readInt();
			headerSize += 4;
			if (version != RankUtils.VERSION) {
				throw new RuntimeException(
						"Different PageRank data version detected. Please recompute PageRank values.");
			}

			// read the thresholds
			int numberOfThresholds = stream.readInt();
			headerSize += 4;
			thresholds = new double[numberOfThresholds];
			for (int idx = 0; idx < thresholds.length; idx++) {
				thresholds[idx] = stream.readDouble();
				headerSize += 8;
			}
		} catch (IOException e) {
			return;
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				// ignore
			}
		}

		size = ((new File(file).length() - headerSize) / SIZE_OF_RECORD);
		assert size / PAGE_SIZE < Integer.MAX_VALUE;
		pages = new Page[(int)(size / PAGE_SIZE) + 1];
		availablePages = MAX_PAGES;
	}
	
	private Page getPage(int pageIndex) throws IOException {
		Page[] pagesArray = pages;
		Page page = pagesArray[pageIndex];
		if (page == null) {
			synchronized (pagesArray) {
				if (pagesArray[pageIndex] == null) {
					// do we have to throw some old page away
					if (availablePages == 0) {
						// find the least used page
						int min = -1;
						for (int idx = 0; idx < pagesArray.length; idx++) {
							if (pagesArray[idx] != null && (min < 0 || pagesArray[idx].hits < pagesArray[min].hits)) {
								min = idx;
							}
						}
						assert min >= 0;
						pagesArray[min] = null;
						availablePages++;
					}
					// always construct a new page so that reference to discarded pages remain valid
					pagesArray[pageIndex] = new Page();
					availablePages--;
				}
				page = pagesArray[pageIndex];
			}
		}
		
		assert page != null;
		
		// local critical section to make sure the page is read only once from disk
		synchronized (page) {
			if (!page.isInitialized) {
				// read the page contents from disk
				try (FileInputStream fis = new FileInputStream(file)) {
					fis.getChannel().position(headerSize + ((long) pageIndex) * PAGE_SIZE * SIZE_OF_RECORD);
					try (DataInputStream in = new DataInputStream(new BufferedInputStream(fis))) {
						long numberOfRanksInPage = Math.min(page.ranks.length, size - (long) pageIndex * PAGE_SIZE);
						for (int idx = 0; idx < numberOfRanksInPage; idx++) {
							in.readInt(); // the node ID is stored but not currently used
							page.ranks[idx] = in.readDouble();
						}
					}
				}
				// mark the page as initialized
				page.isInitialized = true;
			}
			// update page hit statistics
			page.hits++;
		}
		
		return page;
	}

	long size() {
		return size;
	}

	double[] getThresholds() {
		return thresholds;
	}
	
	double read(long id) {
		if (id < 1 || id >= size) {
			return RDFRankProvider.NULL_RANK;
		}

		try {
			Page page = getPage((int) (id / PAGE_SIZE));
			if (page == null) {
				return RDFRankProvider.NULL_RANK;
			}
			return page.ranks[(int) (id % PAGE_SIZE)];
		} catch (IOException iox) {
			return RDFRankProvider.NULL_RANK;
		}
	}
}
