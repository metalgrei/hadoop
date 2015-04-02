package com.metalgrei.example.hadoop.ch1.json.parser;

/**
 * A very loosey-goosey lexer that doesn't enforce any JSON structural rules.
 */
public class JsonLexer {
	
	/** The state. */
	private JsonLexerState state;

	/**
	 * Instantiates a new json lexer.
	 */
	public JsonLexer() {
		this(JsonLexerState.NULL);
	}

	/**
	 * Instantiates a new json lexer.
	 *
	 * @param initState the init state
	 */
	public JsonLexer(JsonLexerState initState) {
		state = initState;
	}

	/**
	 * Gets the state.
	 *
	 * @return the state
	 */
	public JsonLexerState getState() {
		return state;
	}

	/**
	 * Sets the state.
	 *
	 * @param state the new state
	 */
	public void setState(JsonLexerState state) {
		this.state = state;
	}

	/**
	 * The Enum JsonLexerState.
	 */
	public static enum JsonLexerState {
		
		/** The null. */
		NULL, 
 /** The dont care. */
 DONT_CARE, 
 /** The begin object. */
 BEGIN_OBJECT, 
 /** The end object. */
 END_OBJECT, 
 /** The begin string. */
 BEGIN_STRING, 
 /** The end string. */
 END_STRING, 
 /** The inside string. */
 INSIDE_STRING, 
 /** The string escape. */
 STRING_ESCAPE, 
 /** The value separator. */
 VALUE_SEPARATOR, 
 /** The name separator. */
 NAME_SEPARATOR, 
 /** The begin array. */
 BEGIN_ARRAY, 
 /** The end array. */
 END_ARRAY, 
 /** The whitespace. */
 WHITESPACE
	}

	/**
	 * Lex.
	 *
	 * @param c the c
	 */
	public void lex(char c) {
		switch (state) {
		case NULL:
		case BEGIN_OBJECT:
		case END_OBJECT:
		case BEGIN_ARRAY:
		case END_ARRAY:
		case END_STRING:
		case VALUE_SEPARATOR:
		case NAME_SEPARATOR:
		case DONT_CARE:
		case WHITESPACE: {
			if (Character.isWhitespace(c)) {
				state = JsonLexerState.WHITESPACE;
				break;
			}
			switch (c) {
			// value-separator (comma)
			case ',':
				state = JsonLexerState.VALUE_SEPARATOR;
				break;
			// name-separator (colon)
			case ':':
				state = JsonLexerState.NAME_SEPARATOR;
				break;
			// string
			//
			case '"':
				state = JsonLexerState.BEGIN_STRING;
				break;
			// start-object
			//
			case '{':
				state = JsonLexerState.BEGIN_OBJECT;
				break;
			// end-object
			//
			case '}':
				state = JsonLexerState.END_OBJECT;
				break;
			// begin-array
			//
			case '[':
				state = JsonLexerState.BEGIN_ARRAY;
				break;
			// end-array
			//
			case ']':
				state = JsonLexerState.END_ARRAY;
				break;
			default:
				state = JsonLexerState.DONT_CARE;
			}
			break;
		}
		case BEGIN_STRING: {
			state = JsonLexerState.INSIDE_STRING;
			// we will now enter the STRING state below
		}
		case INSIDE_STRING: {
			switch (c) {
			// end-string
			//
			case '"':
				state = JsonLexerState.END_STRING;
				break;
			// escape
			//
			case '\\':
				state = JsonLexerState.STRING_ESCAPE;
			}
			break;
		}
		case STRING_ESCAPE: {
			state = JsonLexerState.INSIDE_STRING;
			break;
		}
		}
	}

}
