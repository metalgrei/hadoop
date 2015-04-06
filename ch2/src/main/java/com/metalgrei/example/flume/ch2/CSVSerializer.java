package com.metalgrei.example.flume.ch2;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.apache.flume.serialization.EventSerializer;

// TODO: Auto-generated Javadoc
/**
 * The Class CSVSerializer.
 */
public class CSVSerializer implements EventSerializer {
	
	/** The Constant LOG. */
	private static final Log LOG = LogFactory
			.getLog(EventSerializer.class);
	
	/** The Constant FORMAT. */
	public static final String FORMAT = "format";
	
	/** The Constant REGEX. */
	public static final String REGEX = "regex";
	
	/** The Constant REGEX_ORDER. */
	public static final String REGEX_ORDER = "regexorder";
	
	/** The default format. */
	private final String DEFAULT_FORMAT = "CSV";
	
	/** The default regex. */
	private final String DEFAULT_REGEX = "(.*)";
	
	/** The default order. */
	private final String DEFAULT_ORDER = "1";
	
	/** The format. */
	private final String format;
	
	/** The regex. */
	private final Pattern regex;
	
	/** The regex order. */
	private final String[] regexOrder;
	
	/** The out. */
	private final OutputStream out;
	
	/** The order indexer. */
	private Map<Integer, ByteBuffer > orderIndexer;
	
	/**
	 * Instantiates a new CSV serializer.
	 *
	 * @param out the out
	 * @param ctx the ctx
	 */
	private CSVSerializer(OutputStream out, Context ctx) {
		this.format = ctx.getString(FORMAT, DEFAULT_FORMAT);
		if (!format.equals(DEFAULT_FORMAT)){
			LOG.warn("Unsupported output format" + format + ", using default instead");
		}
		this.regex = Pattern.compile(ctx.getString(REGEX, DEFAULT_REGEX));
		this.regexOrder = ctx.getString(REGEX_ORDER, DEFAULT_ORDER).split(" ");
		this.out = out;
		orderIndexer = new HashMap<Integer, ByteBuffer>();
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.serialization.EventSerializer#afterCreate()
	 */
	@Override
	public void afterCreate() throws IOException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.serialization.EventSerializer#afterReopen()
	 */
	@Override
	public void afterReopen() throws IOException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.serialization.EventSerializer#flush()
	 */
	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.serialization.EventSerializer#beforeClose()
	 */
	@Override
	public void beforeClose() throws IOException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.serialization.EventSerializer#supportsReopen()
	 */
	@Override
	public boolean supportsReopen() {
		// TODO Auto-generated method stub
		return false;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.flume.serialization.EventSerializer#write(org.apache.flume.Event)
	 */
	@Override
	public void write(Event event) throws IOException {
		Matcher matcher = regex.matcher(new String(event.getBody(), Charsets.UTF_8));
		if (matcher.find()) {
			// first write out the timestamp
			String timestamp = event.getHeaders().get(TimestampInterceptor.Constants.TIMESTAMP);
			if (timestamp == null || timestamp.isEmpty()){
				long now = System.currentTimeMillis();
				timestamp = Long.toString(now);
			}
			out.write(timestamp.getBytes());
			out.write(',');
			// next save the regex group matches into a hash for reodering
			int groupIndex = 0;
			int totalGroups = matcher.groupCount();
			for (int i = 0, count = totalGroups; i < count; i++) {
				groupIndex = i + 1;
				orderIndexer.put(Integer.valueOf(regexOrder[i]), ByteBuffer.wrap(matcher.group(groupIndex).getBytes()));
			}
			// write out the columns of the table
			int i = 1;
			for(Integer key : orderIndexer.keySet()){
				out.write(orderIndexer.get(key).array());
				if (i < totalGroups){
					out.write(',');
				}
				i++;
			}
			out.write('\n');
		}
		else {
			LOG.warn("Message skipped, no regex match: " + event.getBody().toString());
		}
	}
	
	/**
	 * The Class Builder.
	 */
	public static class Builder implements EventSerializer.Builder {
		
		/* (non-Javadoc)
		 * @see org.apache.flume.serialization.EventSerializer.Builder#build(org.apache.flume.Context, java.io.OutputStream)
		 */
		@Override
		public EventSerializer build(Context context, OutputStream out) {
			CSVSerializer s = new CSVSerializer(out, context);
			return s;
		}
	}
	
	

}
