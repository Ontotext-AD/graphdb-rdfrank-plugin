package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.util.BigFloatArray;
import java.util.LinkedList;
import java.util.Locale;

import com.ontotext.GraphDBInternalConfigParameters;
import com.ontotext.config.ParametersSource;

/**
 * Utility methods used by the rank-computing routines
 */
class RankUtils {

	private static final String FORMAT_STRING = ParametersSource.parameters()
			.get(GraphDBInternalConfigParameters.RDFRANK_FORMAT);

	private static final Locale FORMAT_LOCALE = Locale.US;

	static final int MAGIC = 0x10ad77fe;
	static final int VERSION = 1;
	static final int PRECISION = 100;

	/**
	 * Finds the index of the largest threshold in array of thresholds that is still smaller than or equal to
	 * a given number
	 * 
	 * @param number
	 *            we are looking for
	 * @param thresholds
	 *            array of thresholds
	 * @return index in the thresholds array
	 */
	static int findThresholdIndex(double number, double[] thresholds) {
		if (number <= 0) {
			return -1;
		}
		for (int idx = 0; idx < thresholds.length - 1; idx += 2) {
			if (thresholds[idx] <= number && number <= thresholds[idx + 1]) {
				return idx;
			}
		}
		return -1;
	}

	/**
	 * Computes the array of thresholds for a given array of ranks
	 * 
	 * @param ranks
	 *            array of ranks
	 * @param precision
	 *            the minimum factor between thresholds
	 * @return array of thresholds
	 */
	static double[] computeThresholds(BigFloatArray ranks, int precision) {
		// create a sorted copy of the ranks array
		BigFloatArray sorted = ranks.clone();
		sorted.sort();

		int idx = 0;
		double cur, min, max;

		// find the beginning of the non-negative ranks
		while (sorted.get(idx) <= 0 && idx < sorted.length()) {
			idx++;
		}

		// are there any non-zero elements in this array
		if (idx >= sorted.length()) {
			return new double[0];
		}

		assert (idx < sorted.length() && sorted.get(idx) > 0);

		min = max = sorted.get(idx);

		LinkedList<Double> thresholds = new LinkedList<>();

		// determine all thresholds
		while (++idx < sorted.length()) {
			cur = sorted.get(idx);
			if (cur > min * precision) {
				thresholds.add(min);
				thresholds.add(max);
				min = max = cur;
			} else {
				max = cur;
			}
		}
		// add the last element as an upper threshold
		thresholds.add(min);
		thresholds.add(max);

		idx = 0;
		double[] thresholdsArray = new double[thresholds.size()];
		for (Double t : thresholds) {
			thresholdsArray[idx++] = t;
		}

		return thresholdsArray;
	}

	/**
	 * Format a float number
	 * 
	 * @param number
	 * @return formatted string
	 */
	static String format(float number) {
		return String.format(FORMAT_LOCALE, FORMAT_STRING, number);
	}
	
	static String formatWithDigits(double number, int digits) {
		return String.format(FORMAT_LOCALE, "%.0" + digits + "f", number);
	}
}