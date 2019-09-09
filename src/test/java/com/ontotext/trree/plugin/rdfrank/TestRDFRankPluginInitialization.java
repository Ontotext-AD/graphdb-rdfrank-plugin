package com.ontotext.trree.plugin.rdfrank;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.Rule;
import org.junit.Test;

import com.ontotext.test.TemporaryLocalFolder;
import com.ontotext.trree.OwlimSchemaRepository;

public class TestRDFRankPluginInitialization {
	@Rule
	public TemporaryLocalFolder tmp = new TemporaryLocalFolder();

	@Test
	public void testPluginInitFromPartialStorage() throws IOException {
		File folder = tmp.newFolder("rank-storage");
		String dataUpdate = "insert data {<urn:1> <urn:p> <urn:2> . <urn:2> <urn:q> <urn:3> .}";
		// init, add some data, create rank values
		initCreateRankShutdown(folder, dataUpdate);
		// now, remove / corrupt the index
		for (File f : new File(folder, "storage").listFiles()) {
			if (!f.isDirectory() || !"rdfrank".equals(f.getName())) 
				continue;
			// delete ...
			for (File ixd : f.listFiles()) {
				if (ixd.isFile() && ixd.getName().endsWith("index")) {
					System.out.println("truncate "+ixd.getName());
					RandomAccessFile raf = new RandomAccessFile(ixd, "rw");
					// truncate but not set it empty 
					raf.setLength(4);
					raf.close();
				}
					
			}
		}
		// check if it fail - if not fixed 1103 it throws here
		/*
		Caused by: org.eclipse.rdf4j.sail.SailException: java.lang.IllegalStateException: Storage not initialized
		at com.ontotext.trree.SailConnectionImpl.handleTransactionFailure(SailConnectionImpl.java:990)
		at com.ontotext.trree.SailConnectionImpl.commitInternal(SailConnectionImpl.java:639)
		 */
		initCreateRankShutdown(folder, null);
	}
	
	private void initCreateRankShutdown(File folder, String dataUpdate) {
		OwlimSchemaRepository sail = new OwlimSchemaRepository();
		SailRepository rep = new SailRepository(sail);
		rep.setDataDir(folder);
		rep.init();
		String calcRank = "PREFIX rank: <http://www.ontotext.com/owlim/RDFRank#> "
				+ "INSERT DATA "+"{_:anon <" + RDFRank.COMPUTE + "> \"true\"}";
		try {
			SailRepositoryConnection conn = rep.getConnection();
			try {
				if (dataUpdate != null)
					conn.prepareUpdate(dataUpdate).execute();
				conn.prepareUpdate(calcRank).execute();
			} finally {
				conn.close();
			}
		} finally {
			rep.shutDown();
		}
	}
}
