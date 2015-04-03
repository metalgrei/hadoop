package com.metalgrei.example.hadoop.ch1;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Class AbstractCh1Test.
 */
public abstract class AbstractCh1Test {

	private static final Log LOG = LogFactory.getLog(WordCount.class);

	/** The Constant SEPARATOR. */
	private static final String SEPARATOR = System
			.getProperty("file.separator");

	private static final String BASE_PATH = "src/test/resources/";
	
	protected String getSeparator(){
		return SEPARATOR;
	}

	/**
	 * Gets the path.
	 *
	 * @param name
	 *            the name
	 * @return the path
	 */
	protected String getPath(String name) {
		String fullPath = getBasePath() + name;
		StringBuffer stringBuffer = new StringBuffer(Paths.get(fullPath)
				.toAbsolutePath().toString());
		stringBuffer.append(SEPARATOR);
		return stringBuffer.toString();
	}

	protected String getBasePath() {
		return BASE_PATH;
	}

	protected void deleteFolder(String directoryPath) {
		Path directory = Paths.get(directoryPath);
		try {
			Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file,
						BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir,
						IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}

			});
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	protected Boolean comparatorFile(String pathIn, String pathOutput) {
		File fileIn = new File(Paths.get(pathIn).toAbsolutePath().toString());
		File fileOutPut = new File(Paths.get(pathOutput).toAbsolutePath()
				.toString());
		Boolean result = Boolean.FALSE;
		try {
			result = FileUtils.contentEquals(fileIn, fileOutPut);
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
		return result;
	}

}
