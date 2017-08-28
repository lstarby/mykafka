package com.tony.demo;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.guohuai.supervise.handle.AbstractProccessorHandle;

@SuppressWarnings("rawtypes")
public abstract class AbstractStreamProccessor extends AbstractProcessor {

	private String proccessorName;
	
	private AbstractProccessorHandle proccessorHandle;
	
	private ProccessorHandleContext handleContext;
	
	private String parentNames;

	public AbstractProccessorHandle getProccessorHandle() {
		return proccessorHandle;
	}

	public void setProccessorHandle(AbstractProccessorHandle proccessorHandle) {
		this.proccessorHandle = proccessorHandle;
	}

	public ProccessorHandleContext getHandleContext() {
		return handleContext;
	}

	public void setHandleContext(ProccessorHandleContext handleContext) {
		this.handleContext = handleContext;
	}

	public String getProccessorName() {
		return proccessorName;
	}

	public void setProccessorName(String proccessorName) {
		this.proccessorName = proccessorName;
	}

	public String getParentNames() {
		return parentNames;
	}

	public void setParentNames(String parentNames) {
		this.parentNames = parentNames;
	}

	
}
