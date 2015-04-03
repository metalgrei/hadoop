package com.metalgrei.example.hadoop.ch1.json;

import org.junit.Test;

import com.metalgrei.example.hadoop.ch1.AbstractCh1Test;

// TODO: Auto-generated Javadoc
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

}
