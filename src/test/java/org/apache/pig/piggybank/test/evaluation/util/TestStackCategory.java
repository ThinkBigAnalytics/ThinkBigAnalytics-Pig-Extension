/**
 * 
 */
package org.apache.pig.piggybank.test.evaluation.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.util.StackCategory;
import org.junit.Test;

/**
 * Test categorization code.
 * 
 */
public class TestStackCategory {

	private static final String INPUT_DIR = "src/test/java/org/apache/pig/piggybank/test/data/stacktrace";

	/**
	 * Generate pig script from data file to pass in a stackframe for
	 * classification
	 * 
	 * @throws Exception
	 */
	//@Test // not working from maven, but works from Eclipse
	public void testStackframeLoad() throws Exception {
		PigServer pig = new PigServer(ExecType.LOCAL);
		
		pig.registerJar("lib/json-simple-1.1.1.jar");
		pig.registerJar("lib/jackson-core-asl-1.5.2.jar");
		pig.registerJar("lib/jackson-mapper-asl-1.5.2.jar");
		pig.registerJar("lib/avro-1.5.4.jar");
		pig.registerJar("lib/piggybank.jar");

		pig.registerQuery("a = load '"
				+ INPUT_DIR
				+ "'  USING org.apache.pig.piggybank.storage.avro.AvroStorage(); \n"
				+ "b = foreach a generate stackframe.threadDescriptorArray.stackframeArray.$0;\n"
				+ "c = foreach b generate FLATTEN($0);\n"
				+ "d = foreach c generate org.apache.pig.piggybank.evaluation.util.StackCategory(stackframeArray) as framex;"

		);
		Iterator<Tuple> iter = pig.openIterator("d");

		assertNotNull(iter);
		assertTrue(iter.hasNext());

		while (iter.hasNext()) {
			Tuple t = iter.next();

			assertNotNull(t);
			String val = (String) t.get(0);
			//assertNotNull(val);
			System.out.println(val);
		}

	}

	//@Test
	public void test1() throws IOException {
		StackCategory sc = new StackCategory();
		List<Tuple> b = new ArrayList<Tuple>(2);
		{
			Tuple t = TupleFactory.getInstance().newTuple(1);
			t.set(0, "org.apache.pig.util");
			b.add(t);
		}
		{
			Tuple t = TupleFactory.getInstance().newTuple(1);
			t.set(0, "java.io.Input.fake");
			b.add(t);
		}
		Tuple t2 = TupleFactory.getInstance().newTuple(b);
		String out = sc.exec(t2);
		assertNotNull(out);
		String val = out;
		assertEquals("pig", val);
		System.out.println(val);
	}

	// @Test
	public void test2() throws IOException {
		StackCategory sc = new StackCategory();
		Tuple t = TupleFactory.getInstance().newTuple(1);
		t.set(0, "java.io.File.madeup.package");
		String out = sc.exec(t);
		assertNotNull(out);
		String val = out;
		assertEquals("io", val);
		System.out.println(val);
	}

	// @Test
	public void test3() throws IOException {
		StackCategory sc = new StackCategory();
		Tuple t = TupleFactory.getInstance().newTuple(1);
		t.set(0, "com.corp.process");
		String out = sc.exec(t);
		assertNotNull(out);
		String val = out;
		assertEquals("user", val);
		System.out.println(val);
	}

}
