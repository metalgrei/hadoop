package com.metalgrei.example.hadoop.ch1.json;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.metalgrei.example.hadoop.ch1.AbstractCh1Test;

/**
 * The Class ExampleJsonJobTest.
 */
public class ExampleJsonJobTest extends  AbstractCh1Test{
	
	/**
	 * Main whitout path test.
	 *
	 * @throws Exception the exception
	 */
	@Test(expected = NullPointerException.class)
	public final void mainWhitoutPathTest() throws Exception {
		String[] args = new String[2];
		ExampleJsonJob.main(args);
	}
	
	/**
	 * Main safe null test.
	 *
	 * @throws Exception the exception
	 */
	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public final void mainSafeNullTest() throws Exception {
		String[] args = null;
		ExampleJsonJob.main(args);
	}
	
	/**
	 * Main one parameter test.
	 *
	 * @throws Exception the exception
	 */
	@Test(expected = NullPointerException.class)
	public final void mainOneParameterTest() throws Exception {
		String[] args = new String[1];
		ExampleJsonJob.main(args);
	}
	
	/**
	 * Main three parameter test.
	 *
	 * @throws Exception the exception
	 */
	@Test(expected = NullPointerException.class)
	public final void mainThreeParameterTest() throws Exception {
		String[] args = new String[3];
		ExampleJsonJob.main(args);
	}
	
	@Test
	public final void maiTest() throws Exception {
		String fullPath = getPath("com/metalgrei/example/hadoop/ch1/json/");
		String[] args = new String[2];
		args[0] = fullPath + "input/input.txt";
		args[1] = fullPath + "output";

		ExampleJsonJob.main(args);

		assertTrue(comparatorFile(args[1] + getSeparator() + "part-r-00000",
				fullPath + "output.txt"));
	}

}
