package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.util.BigFloatArray;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.ontotext.trree.util.FileUtils;

/**
 * Provides writing functionality for RDFRank plug-in storage file
 */
class FileRankWriter {
	private String rankFile;

	FileRankWriter(String rankFile) {
		this.rankFile = rankFile;
	}

	/**
	 * Writes array of ranks into file
	 * 
	 * @param ranks
	 *            array of ranks
	 */
	void write(BigFloatArray ranks) {
		DataOutputStream dos = null;
		try {
			dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(rankFile))));
			// compute the number of thresholds
			double[] thresholds = RankUtils.computeThresholds(ranks, RankUtils.PRECISION);
			// write down the header of the PageRank file
			dos.writeInt(RankUtils.MAGIC);
			dos.writeInt(RankUtils.VERSION);
			dos.writeInt(thresholds.length);
			for (double threshold : thresholds) {
				dos.writeDouble(threshold);
			}
			// read and write the PageRanks
			for (int node = 0; node < ranks.length(); node++) {
				dos.writeInt(node);
				dos.writeDouble(ranks.get(node));
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed writing rank to file '" + rankFile + "': " + e.getMessage());
		} finally {
			FileUtils.closeQuietly(dos);
		}
	}
}
