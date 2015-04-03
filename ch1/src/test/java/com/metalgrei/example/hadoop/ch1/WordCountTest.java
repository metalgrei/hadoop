package com.metalgrei.example.hadoop.ch1;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * The Class WordCountTest.
 */
public class WordCountTest extends AbstractCh1Test {

	/**
	 * Main whitout path test.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test(expected = IllegalArgumentException.class)
	public final void mainWhitoutPathTest() throws Exception {
		String[] args = new String[2];
		WordCount.main(args);
	}

	/**
	 * Main safe null test.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test(expected = IllegalArgumentException.class)
	public final void mainSafeNullTest() throws Exception {
		String[] args = null;
		WordCount.main(args);
	}

	/**
	 * Main one parameter test.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test(expected = IllegalArgumentException.class)
	public final void mainOneParameterTest() throws Exception {
		String[] args = new String[1];
		WordCount.main(args);
	}

	/**
	 * Main three parameter test.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test(expected = IllegalArgumentException.class)
	public final void mainThreeParameterTest() throws Exception {
		String[] args = new String[3];
		WordCount.main(args);
	}

	@Test
	public final void maiTest() throws Exception {
		String fullPath = getPath("com/metalgrei/example/hadoop/ch1/");
		String[] args = new String[2];
		args[0] = fullPath + "file?";
		args[1] = fullPath + "out";

		WordCount.main(args);

		assertTrue(comparatorFile(args[1] + getSeparator() + "part-r-00000",
				fullPath + "output.txt"));
		deleteFolder(args[1]);
	}

}
