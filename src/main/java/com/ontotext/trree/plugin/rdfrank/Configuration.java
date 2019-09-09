package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.trree.sdk.PluginException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ontotext.trree.plugin.rdfrank.Configuration.RDFRankProperty.*;

/**
 * Represents the configuration/state properties of the RDF Rank. The properties are persisted to a file.
 */
public class Configuration {

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	private final String file;
	private Properties properties;

	public Configuration(String file) {
		this.file = file;
	}

	public void initialize() {
		loadProperties();
	}

	public void save() {
		persistProperties();
	}

	public long getLastRankedId() {
		return Long.parseLong(getProperty(LAST_RANKED_ID_PROPERTY));
	}

	public void setLastRankedId(long value) {
		setProperty(LAST_RANKED_ID_PROPERTY, Long.toString(value));
	}

	public double getMinRank() {
		return Double.parseDouble(getProperty(MIN_RANK_PROPERTY));
	}

	public void setMinRank(double value) {
		setProperty(MIN_RANK_PROPERTY, Double.toString(value));
	}

	public double getMaxRank() {
		return Double.parseDouble(getProperty(MAX_RANK_PROPERTY));
	}

	public void setMaxRank(double value) {
		setProperty(MAX_RANK_PROPERTY, Double.toString(value));
	}

	public boolean getFilteringEnabled() {
		return Boolean.parseBoolean(getProperty(FILTERING));
	}

	public void setFilteringEnabled(boolean value) {
		setProperty(FILTERING, Boolean.toString(value));
	}

	public boolean getIncludeExplicit() {
		return Boolean.parseBoolean(getProperty(INCLUDE_EXPLICIT));
	}

	public void setIncludeExplicit(boolean value) {
		setProperty(INCLUDE_EXPLICIT, Boolean.toString(value));
	}

	public boolean getIncludeImplicit() {
		return Boolean.parseBoolean(getProperty(INCLUDE_IMPLICIT));
	}

	public void setIncludeImplicit(boolean value) {
		setProperty(INCLUDE_IMPLICIT, Boolean.toString(value));
	}

	public Collection<Value> getIncludedPredicates() {
		return getValueCollection(INCLUDED_PREDICATES_PROPERTY);
	}

	public void setIncludedPredicates(Collection<Value> value) {
		setValueCollection(INCLUDED_PREDICATES_PROPERTY, value);
	}

	public Collection<Value> getIncludedGraphs() {
		return getValueCollection(INCLUDED_GRAPHS_PROPERTY);
	}

	public void setIncludedGraphs(Collection<Value> value) {
		setValueCollection(INCLUDED_GRAPHS_PROPERTY, value);
	}

	public Collection<Value> getExcludedPredicates() {
		return getValueCollection(EXCLUDED_PREDICATES_PROPERTY);
	}

	public void setExcludedPredicates(Collection<Value> value) {
		setValueCollection(EXCLUDED_PREDICATES_PROPERTY, value);
	}

	public Collection<Value> getExcludedGraphs() {
		return getValueCollection(EXCLUDED_GRAPHS_PROPERTY);
	}

	public void setExcludedGraphs(Collection<Value> value) {
		setValueCollection(EXCLUDED_GRAPHS_PROPERTY, value);
	}

	public int getLastConfigCash() {
		return Integer.parseInt(getProperty(COMPUTED_CONFIG_HASH));
	}

	public void setComputedConfigCash(int value) {
		setProperty(COMPUTED_CONFIG_HASH, String.valueOf(value));
	}

	public Collection<Value> getValueCollection(RDFRankProperty property) {
		return Stream
				.of(getProperty(property).split("\n"))
				.filter(iri -> !iri.equals(""))
				.map(VF::createIRI)
				.collect(Collectors.toList());
	}

	public void setValueCollection(RDFRankProperty property, Collection<? extends Value> value) {
		setProperty(property, value
				.stream()
				.map(String::valueOf)
				.collect(Collectors.joining("\n")));
	}

	/**
	 * Loads the properties object from the file storage
	 * <li>If the file exists we load the properties from it</li>
	 * <li>If the file is absent we create it and persist new default properties instance</li>
	 */
	private void loadProperties() {
		properties = new Properties();

		File propertiesFile = new File(file);
		if (propertiesFile.exists()) {
			try (BufferedReader reader = Files.newBufferedReader(propertiesFile.toPath())) {
				properties.load(reader);
			} catch (IOException e) {
				throw new PluginException("Can't load RDFRank properties", e);
			}
		} else {
			fillWithDefaultValues();
			persistProperties();
		}
	}

	private void fillWithDefaultValues() {
		setProperty(LAST_RANKED_ID_PROPERTY, "0");
		setProperty(MIN_RANK_PROPERTY, "");
		setProperty(MAX_RANK_PROPERTY, "");
		setProperty(FILTERING, "false");
		setProperty(INCLUDED_PREDICATES_PROPERTY, "");
		setProperty(INCLUDED_GRAPHS_PROPERTY, "");
		setProperty(EXCLUDED_PREDICATES_PROPERTY, "");
		setProperty(EXCLUDED_GRAPHS_PROPERTY, "");
		setProperty(INCLUDE_EXPLICIT, "true");
		setProperty(INCLUDE_IMPLICIT, "true");
		setProperty(COMPUTED_CONFIG_HASH, "0");
	}

	/**
	 * Persists the properties object to a file
	 */
	private void persistProperties() {
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(file))) {
			properties.store(writer, "RDFRank properties");
		} catch (IOException e) {
			throw new PluginException("Can't load RDFRank properties", e);
		}
	}

	private String getProperty(RDFRankProperty property) {
		return properties.get(property.toString()).toString();
	}

	private void setProperty(RDFRankProperty property, String value) {
		properties.setProperty(property.toString(), value);
	}

	@Override
	public int hashCode() {
		int hash = Objects.hash(
				getIncludedPredicates(),
				getIncludedGraphs(),
				getExcludedPredicates(),
				getExcludedGraphs(),
				getIncludeExplicit(),
				getIncludeImplicit());
		hash = getFilteringEnabled() ? hash | 1 : hash & ~1;

		return hash;
	}

	public enum RDFRankProperty {
		LAST_RANKED_ID_PROPERTY("lastRankedID"),
		MIN_RANK_PROPERTY("minRank"),
		MAX_RANK_PROPERTY("maxRank"),
		FILTERING("filtering"),
		INCLUDED_PREDICATES_PROPERTY("includedPredicates"),
		INCLUDED_GRAPHS_PROPERTY("includedGraphs"),
		EXCLUDED_PREDICATES_PROPERTY("excludedPredicates"),
		EXCLUDED_GRAPHS_PROPERTY("excludedGraphs"),
		INCLUDE_EXPLICIT("includeExplicit"),
		INCLUDE_IMPLICIT("includeImplicit"),
		COMPUTED_CONFIG_HASH("computedConfigHash");

		private String name;

		RDFRankProperty(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}
}
