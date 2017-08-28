package com.tony.demo;

import java.util.Objects;

import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guohuai.supervise.handle.common.SuperviseKafkaDataService;
import com.guohuai.supervise.handle.common.SuperviseProccessorKafkaStreamData;
import com.guohuai.supervise.kafka.processors.ProccessorHandleContext;

public abstract class AbstractProccessorHandle  extends SuperviseKafkaDataService implements SuperviseProccessorKafkaStreamData{
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractProccessorHandle.class);
	
	public String convertObjToString(Object key) {
		try {
			if(Objects.isNull(key)) {
				return null;
			}
			return Objects.toString(key);
		} catch (Exception e) {
			logger.error("【Object 对象转换字符串失败】", e);
			return null;
		}
	}
	
	public byte[] convertObjToByteArray(Object value) {
		try {
			if(Objects.isNull(value)) {
				return null;
			}
			Bytes bytes = (Bytes)value;
			
			return bytes.get();
		} catch (Exception e) {
			logger.error("【Object对象转换byte数组失败】", e);
			return null;
		}
	}
	

	public abstract void handle(Object key, Object value, ProccessorHandleContext context) ;
}
