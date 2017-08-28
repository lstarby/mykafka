package com.tony.demo;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author zzw
 *
 */
//@Service
public class TestStreamProcessor extends AbstractProcessor {

	private static final Logger logger = LoggerFactory.getLogger(TestStreamProcessor.class);

	private ProcessorContext context;

	private KeyValueStore<String, String> superviseStreamStore;

	@Override
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.context = context;
		this.context.schedule(10000);
		//superviseStreamStore = (KeyValueStore<String, String>) this.context.getStateStore("supervise-stream-store");
		//Objects.requireNonNull(superviseStreamStore, "State store can't be null");
		//superviseStreamStore.flush();

	}

	@Override
	public void process(Object key, Object value) {
		String keyStr = new String((byte[])key);
		System.out.println("【key:"+ keyStr +"】");
		String valueStr = new String((byte[])value);
		System.out.println("【value:"+ valueStr +"】");
		System.out.println("【ApplicationId="+ context.applicationId() +", TaskId="+ context.taskId() + ", Partition=" + context.partition() + ", Topic="+ context.topic() + ", Timestamp="+ context.timestamp() +", Offset="+ context.offset()  +"】");
/*		context.forward(key, value);
		context.commit();
*/
	}

	@Override
	public void punctuate(long timestamp) {

	}

	@Override
	public void close() {
		try {

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
