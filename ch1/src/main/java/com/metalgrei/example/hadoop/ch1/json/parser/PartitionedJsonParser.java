package com.metalgrei.example.hadoop.ch1.json.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static com.metalgrei.example.hadoop.ch1.json.parser.JsonLexer.JsonLexerState;

/**
 * The Class PartitionedJsonParser.
 */
public class PartitionedJsonParser {

	/** The is. */
	private final InputStream is;
	
	/** The input stream reader. */
	private final InputStreamReader inputStreamReader;
	
	/** The lexer. */
	private final JsonLexer lexer;
	
	/** The bytes read. */
	private long bytesRead = 0;
	
	/** The end of stream. */
	private boolean endOfStream;

	/**
	 * Instantiates a new partitioned json parser.
	 *
	 * @param is the is
	 */
	public PartitionedJsonParser(InputStream is) {
		this.is = is;
		this.lexer = new JsonLexer();
		// You need to wrap the InputStream with an InputStreamReader,
		// so that it can encode the incoming byte stream as UTF-8 characters
		this.inputStreamReader = new InputStreamReader(is,
				StandardCharsets.UTF_8);
	}

	/**
	 * Scan to first begin object.
	 *
	 * @return true, if successful
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private boolean scanToFirstBeginObject() throws IOException {
		// seek until we hit the first begin-object
		//
		char prev = ' ';
		int i;
		while ((i = inputStreamReader.read()) != -1) {
			char c = (char) i;
			bytesRead++;
			if (c == '{' && prev != '\\') {
				lexer.setState(JsonLexer.JsonLexerState.BEGIN_OBJECT);
				return true;
			}
			prev = c;
		}
		endOfStream = true;
		return false;
	}

	/**
	 * The Enum MemberSearchState.
	 */
	private enum MemberSearchState {
		
		/** The found string name. */
		FOUND_STRING_NAME, 
 /** The searching. */
 SEARCHING, 
 /** The in matching object. */
 IN_MATCHING_OBJECT
	}

	/** The Constant inStringStates. */
	private static final EnumSet<JsonLexerState> inStringStates = EnumSet.of(
			JsonLexerState.INSIDE_STRING, JsonLexerState.STRING_ESCAPE);

	/**
	 * Next object containing member.
	 *
	 * @param member the member
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String nextObjectContainingMember(String member) throws IOException {
		if (endOfStream) {
			return null;
		}
		int i;
		int objectCount = 0;
		StringBuilder currentObject = new StringBuilder();
		StringBuilder currentString = new StringBuilder();
		MemberSearchState memberState = MemberSearchState.SEARCHING;
		List<Integer> objectStack = new ArrayList<Integer>();
		if (!scanToFirstBeginObject()) {
			return null;
		}
		currentObject.append("{");
		objectStack.add(0);
		while ((i = inputStreamReader.read()) != -1) {
			char c = (char) i;
			bytesRead++;
			lexer.lex(c);
			currentObject.append(c);
			switch (memberState) {
			case SEARCHING:
				if (lexer.getState() == JsonLexerState.BEGIN_STRING) {
					// we found the start of a string, so reset our string
					// buffer
					//
					currentString.setLength(0);
				} else if (inStringStates.contains(lexer.getState())) {
					// we're still inside a string, so keep appending to our
					// buffer
					//
					currentString.append(c);
				} else if (lexer.getState() == JsonLexerState.END_STRING
						&& member.equals(currentString.toString())) {
					if (objectStack.size() > 0) {
						// we hit the end of the string and it matched the
						// member name (yay)
						//
						memberState = MemberSearchState.FOUND_STRING_NAME;
						currentString.setLength(0);
					}
				} else if (lexer.getState() == JsonLexerState.BEGIN_OBJECT) {
					// we are searching and found a '{', so we reset the current
					// object string
					//
					if (objectStack.size() == 0) {
						currentObject.setLength(0);
						currentObject.append("{");
					}
					objectStack.add(currentObject.length() - 1);
				} else if (lexer.getState() == JsonLexerState.END_OBJECT) {
					if (objectStack.size() > 0) {
						objectStack.remove(objectStack.size() - 1);
					}
					if (objectStack.size() == 0) {
						currentObject.setLength(0);
					}
				}
				break;
			case FOUND_STRING_NAME:
				// keep popping whitespaces until we hit a different token
				//
				if (lexer.getState() != JsonLexerState.WHITESPACE) {
					if (lexer.getState() == JsonLexerState.NAME_SEPARATOR) {
						// found our member!
						//
						memberState = MemberSearchState.IN_MATCHING_OBJECT;
						objectCount = 0;
						if (objectStack.size() > 1) {
							currentObject.delete(0,
									objectStack.get(objectStack.size() - 1));
						}
						objectStack.clear();
					} else {
						// we didn't find a value-separator (:), so our string
						// wasn't a member string
						//
						// keep searching
						//
						memberState = MemberSearchState.SEARCHING;
					}
				}
				break;
			case IN_MATCHING_OBJECT:
				if (lexer.getState() == JsonLexerState.BEGIN_OBJECT) {
					objectCount++;
				} else if (lexer.getState() == JsonLexerState.END_OBJECT) {
					objectCount--;
					if (objectCount < 0) {
						// we're done! we reached an "}" which is at the same
						// level as the member we
						// found
						//
						return currentObject.toString();
					}
				}
				break;
			}
			// System.out.println("Char '" + c + "', lexer " + lexer.getState()
			// + " member " + memberState + " maxObjectLengthExceeded" +
			// maxObjectLengthExceeded);
		}
		endOfStream = true;
		return null;
	}

	/**
	 * Gets the bytes read.
	 *
	 * @return the bytes read
	 */
	public long getBytesRead() {
		return bytesRead;
	}

	/**
	 * Checks if is end of stream.
	 *
	 * @return true, if is end of stream
	 */
	public boolean isEndOfStream() {
		return endOfStream;
	}

}
