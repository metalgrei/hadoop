package com.metalgrei.example.hadoop.ch1.comparator;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.metalgrei.example.hadoop.ch1.AbstractCh1Test;

public class AirQualityManagerTest extends AbstractCh1Test {
	
	@Test(expected = NullPointerException.class)
	public final void mainWhitoutPathTest() throws Exception {
		String[] args = new String[2];
		AirQualityManager.main(args);
	}
	
	@Test
	public final void maiTest() throws Exception {
		String fullPath = getPath("com/metalgrei/example/hadoop/ch1/comparator/");
		String[] args = new String[3];
		args[0] = fullPath + "Calidad_del_aire.csv";
		args[1] = fullPath + "out";
		args[2] = "measureType";

		AirQualityManager.main(args);

		assertTrue(comparatorFile(args[1] + getSeparator() + "part-r-00000",
				fullPath + "output.txt"));
		deleteFolder(args[1]);
	}

}
