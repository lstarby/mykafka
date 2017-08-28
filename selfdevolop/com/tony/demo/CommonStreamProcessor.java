package com.tony.demo;

import java.util.Objects;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.guohuai.supervise.handle.AbstractProccessorHandle;

/**
 * @author zzw
 *
 */
//@Service
public class CommonStreamProcessor extends AbstractStreamProccessor {

	private static final Logger logger = LoggerFactory.getLogger(CommonStreamProcessor.class);

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
		String threadName = Thread.currentThread().getName();
		System.out.println("【ThreadName:"+ threadName +", ApplicationId="+ context.applicationId() +", TaskId="+ context.taskId() + ", Partition=" + context.partition() + ", Topic="+ context.topic() + ", Timestamp="+ context.timestamp() +", Offset="+ context.offset()  +"】");
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
