package com.metalgrei.example.flume.ch2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Assert;
import org.junit.Test;

public class CSVSerializerTest {

	private File testFile = new File("src/test/resources/events.txt");
	private String msgText1 = "192.168.1.3 Netscreen-FW1: NetScreen device_id=Netscreen-FW1 [Root]system-notification-00257(traffic): start_time=\"2008-11-05 23:56:32\" duration=0 policy_id=125 service=syslog proto=17 src zone=Untrust dst zone=Trust action=Deny sent=0 rcvd=0 src=172.26.1.75 dst=166.2.3.50 src_port=514 dst_port=514 session_id=0";
	private String msgText2 = "172.16.10.42 ns5gt: NetScreen device_id=ns5gt  [No Name]system-notification-00257(traffic): start_time=\"2005-03-16 16:33:22\" duration=0 policy_id=320001 service=tcp/port:120 proto=6 src zone=Null dst zone=self action=Deny sent=0 rcvd=60 src=192.168.2.1 dst=1.2.3.4 src_port=31048 dst_port=12";

	@Test
	public void testCSVAndColumns1() throws Exception {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(TimestampInterceptor.Constants.TIMESTAMP, "1390883411761");
		OutputStream out = new FileOutputStream(testFile);
		Context context = new Context();
		context.put(CSVSerializer.FORMAT, "CSV");
		context.put(CSVSerializer.REGEX,
				".* proto=(\\d+) .* src=(.*) dst=(.*) src_port=(\\d+) dst_port=(\\d+).*");
		context.put(CSVSerializer.REGEX_ORDER, "5 1 2 3 4");
		EventSerializer serializer = EventSerializerFactory.getInstance(
				"com.metalgrei.example.flume.ch2.CSVSerializer$Builder", context,
				out);
		serializer.afterCreate();
		serializer.write(EventBuilder.withBody(msgText1, Charsets.UTF_8,
				headers));
		serializer.flush();
		serializer.beforeClose();
		out.flush();
		out.close();

		BufferedReader reader = new BufferedReader(new FileReader(testFile));
		Assert.assertEquals("1390883411761,172.26.1.75,166.2.3.50,514,514,17",
				reader.readLine());
		reader.close();

		FileUtils.forceDelete(testFile);
	}
	
	@Test
	public void testCSVAndColumns2() throws Exception {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(TimestampInterceptor.Constants.TIMESTAMP, "1390883411761");
		OutputStream out = new FileOutputStream(testFile);
		Context context = new Context();
		context.put(CSVSerializer.FORMAT, "CSV");
		context.put(CSVSerializer.REGEX, ".* proto=(\\d+) .* src=(.*) dst=(.*) src_port=(\\d+) dst_port=(\\d+).*");
		context.put(CSVSerializer.REGEX_ORDER, "5 1 2 3 4");
		
		EventSerializer serializer =
				EventSerializerFactory.getInstance("com.metalgrei.example.flume.ch2.CSVSerializer$Builder", context, out);
		serializer.afterCreate();
		serializer.write(EventBuilder.withBody(msgText2, Charsets.UTF_8, headers));
		serializer.flush();
		serializer.beforeClose();
		out.flush();
		out.close();

		BufferedReader reader = new BufferedReader(new FileReader(testFile));
		Assert.assertEquals("1390883411761,192.168.2.1,1.2.3.4,31048,12,6", reader.readLine());
		reader.close();

		FileUtils.forceDelete(testFile);
	}

}
