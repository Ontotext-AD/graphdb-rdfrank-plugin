package com.ontotext.trree.plugin.rdfrank;

import com.ontotext.graphdb.Config;
import com.ontotext.test.TemporaryLocalFolder;
import com.ontotext.test.utils.Utils;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestPluginRDFRank extends TestPluginRDFRankBase {

	@ClassRule
	public static TemporaryLocalFolder tmpFolder = new TemporaryLocalFolder();

	public TestPluginRDFRank(boolean useUpdate) {
		super(useUpdate);
	}

	@BeforeClass
	public static void setWorkDir() {
		System.setProperty("graphdb.home.work", String.valueOf(tmpFolder.getRoot()));
		Config.reset();
	}

	@AfterClass
	public static void resetWorkDir() {
		System.clearProperty("graphdb.home.work");
		Config.reset();
	}

	@Test
	public void test1() {
		load("TestPluginRDFRank1.nt");
		check("TestPluginRDFRank1.out");
	}

	@Test
	public void test2() {
		load("TestPluginRDFRank2.rdf");
		check("TestPluginRDFRank2.out");
	}

	@Test
	public void test3() {
		load("TestPluginRDFRank1.nt");
		Iterator<BindingSet> iter = Utils.evaluateQuery(sailConn,
				"PREFIX ns: <http://www.ontotext.com/owlim/rdfrank/test#> "
						+ "PREFIX rank: <http://www.ontotext.com/owlim/RDFRank#> "
						+ "SELECT ?o ?r WHERE {ns:a ?p ?o . ?o rank:hasRDFRank ?r}", QueryLanguage.SPARQL,
				true);
		while (iter.hasNext()) {
			BindingSet bs = iter.next();
			if (bs.getValue("o").stringValue().equals("http://www.ontotext.com/owlim/rdfrank/test#b")) {
				assertTrue(bs.getValue("r").stringValue().equals("0.25974"));
			} else if (bs.getValue("o").stringValue().equals("http://www.ontotext.com/owlim/rdfrank/test#c")) {
				assertTrue(bs.getValue("r").stringValue().equals("1.00000"));
			} else {
				fail("Unexpected binding: " + bs.getValue("o"));
			}
		}
	}

	@Test
	public void testLeaks() {
		load("TestPluginRDFRank2.rdf");
		for (int idx = 0; idx < 35; idx++) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			recomputeRank(DEFAULT_EPSILON, DEFAULT_ITERATIONS);
		}

	}

	@Test
	public void testNonDefaultPrecision3() {
		load("TestPluginRDFRank1.nt");
		checkNonDefaultPrecision(3, "0.260", "1.000");
	}
	@Test
	public void testNonDefaultPrecision4() {
		load("TestPluginRDFRank1.nt");
		checkNonDefaultPrecision(4, "0.2597", "1.0000");
	}
	@Test
	public void testNonDefaultPrecision5() {
		load("TestPluginRDFRank1.nt");
		checkNonDefaultPrecision(5, "0.25974", "1.00000");
	}
}
