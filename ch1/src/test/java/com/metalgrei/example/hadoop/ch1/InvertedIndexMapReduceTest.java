package com.metalgrei.example.hadoop.ch1;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class InvertedIndexMapReduceTest extends AbstractCh1Test {
	
	@Test(expected = IllegalArgumentException.class)
	public final void mainWhitoutPathTest() throws Exception {
		String[] args = new String[2];
		InvertedIndexMapReduce.main(args);
	}
	
	@Test(expected = NullPointerException.class)
	public final void mainSafeNullTest() throws Exception {
		String[] args = null;
		InvertedIndexMapReduce.main(args);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public final void mainOneParameterTest() throws Exception {
		String[] args = new String[1];
		InvertedIndexMapReduce.main(args);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public final void mainThreeParameterTest() throws Exception {
		String[] args = new String[3];
		InvertedIndexMapReduce.main(args);
	}
	
	@Test
	public final void maiTest() throws Exception {
		String fullPath = getPath("com/metalgrei/example/hadoop/ch1/");
		String[] args = new String[2];
		args[0] = fullPath + "file?";
		args[1] = fullPath + "invertedIndexMapReduceOutput";

		InvertedIndexMapReduce.main(args);

		assertTrue(comparatorFile(args[1] + getSeparator() + "part-r-00000",
				fullPath + "invertedIndexMapReduceOutput.txt"));
		deleteFolder(args[1]);
	}

}
