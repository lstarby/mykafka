package com.tony.demo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.guohuai.supervise.utils.xml.XmlKafka.Sink;

public class ProccessorHandleContext {

	
	private ProcessorContext processorContext;
	
	private Deserializer keyDeserializer;
	
	private Serializer keySerializer;
	
	private Deserializer valueDeserializer;
	
	private Serializer valueSerializer;
	
	private KafkaStreams kafkaStreams;
	
	private Class<?> valueClass;
	
	private Class<?> keyClass;
	
	private StreamsConfig streamsConfig;
	
	// private String topicName;
	
	private String[] topicNameArray;
	
	private String applicationId;
	
	private Map<String, AbstractStreamProccessor> proccessorMap;
	
	private List<Sink> sinkList;
	
	private String sourceName;

	public ProcessorContext getProcessorContext() {
		return processorContext;
	}

	public void setProcessorContext(ProcessorContext processorContext) {
		this.processorContext = processorContext;
	}
	
	
	public Deserializer getKeyDeserializer() {
		return keyDeserializer;
	}

	public void setKeyDeserializer(Deserializer keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
	}

	public Serializer getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(Serializer keySerializer) {
		this.keySerializer = keySerializer;
	}

	public Deserializer getValueDeserializer() {
		return valueDeserializer;
	}

	public void setValueDeserializer(Deserializer valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}

	public Serializer getValueSerializer() {
		return valueSerializer;
	}

	public void setValueSerializer(Serializer valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	public KafkaStreams getKafkaStreams() {
		return kafkaStreams;
	}

	public void setKafkaStreams(KafkaStreams kafkaStreams) {
		this.kafkaStreams = kafkaStreams;
	}

	public Class<?> getValueClass() {
		return valueClass;
	}

	public void setValueClass(Class<?> valueClass) {
		this.valueClass = valueClass;
	}

	public Class<?> getKeyClass() {
		return keyClass;
	}

	public void setKeyClass(Class<?> keyClass) {
		this.keyClass = keyClass;
	}

	public StreamsConfig getStreamsConfig() {
		return streamsConfig;
	}

	public void setStreamsConfig(StreamsConfig streamsConfig) {
		this.streamsConfig = streamsConfig;
	}

	public String[] getTopicNameArray() {
		return topicNameArray;
	}

	public void setTopicNameArray(String[] topicNameArray) {
		this.topicNameArray = topicNameArray;
	}

	public String getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public Map<String, AbstractStreamProccessor> getProccessorMap() {
		return proccessorMap;
	}

	public void setProccessorMap(Map<String, AbstractStreamProccessor> proccessorMap) {
		this.proccessorMap = proccessorMap;
	}
	

	public List<Sink> getSinkList() {
		return sinkList;
	}

	public void setSinkList(List<Sink> sinkList) {
		this.sinkList = sinkList;
	}
	

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	@Override
	public String toString() {
		return "ProccessorHandleContext [processorContext=" + processorContext + ", keyDeserializer=" + keyDeserializer
				+ ", keySerializer=" + keySerializer + ", valueDeserializer=" + valueDeserializer + ", valueSerializer="
				+ valueSerializer + ", kafkaStreams=" + kafkaStreams + ", valueClass=" + valueClass + ", keyClass="
				+ keyClass + ", streamsConfig=" + streamsConfig + ", topicNameArray=" + Arrays.toString(topicNameArray)
				+ ", applicationId=" + applicationId + ", proccessorMap=" + proccessorMap + ", sinkList=" + sinkList
				+ ", sourceName=" + sourceName + "]";
	}

	
	
}
