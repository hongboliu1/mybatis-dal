package com.bohaisoft.dal.client.audit.message;//package com.yihaodian.ydal.client.audit.message;
//
//import java.util.List;
//import java.util.Map;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.DisposableBean;
//import org.springframework.beans.factory.InitializingBean;
//
//import com.yihaodian.architecture.jumper.common.inner.exceptions.SendFailedException;
//import com.yihaodian.architecture.jumper.common.message.Destination;
//import com.yihaodian.architecture.jumper.producer.Producer;
//import com.yihaodian.architecture.jumper.producer.impl.ProducerFactoryImpl;
//
//public class JumperMQSender implements IMessageSender, InitializingBean, DisposableBean {
//	static transient Logger logger = LoggerFactory.getLogger(JumperMQSender.class);
//	private Producer producer;
//	private String queueName;
//	private boolean enable = true;
//
//	public boolean isEnable() {
//		return enable;
//	}
//
//	public void setEnable(boolean enable) {
//		this.enable = enable;
//	}
//
//	public String getQueueName() {
//		return queueName;
//	}
//
//	public void setQueueName(String queueName) {
//		this.queueName = queueName;
//	}
//
//	public void send(List<Map<String, Object>> list) {
//		if(enable) {
//			if (producer == null) {
//				createProducer();
//			}
//			if (producer != null) {
//				try {
//					producer.sendMessage(list);
//				} catch (SendFailedException e) {
//					logger.error("Error while sending message to JumperMQ," + e.toString());
//				}
//			}
//		}
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see
//	 * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
//	 */
//	@Override
//	public void afterPropertiesSet() throws Exception {
//		if(enable) {
//			createProducer();
//		}
//	}
//
//	private void createProducer() {
//		try {
//			producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic(queueName));
//		} catch (Exception e) {
//			logger.error("Error while creating producer of jumperMQ," + e.toString());
//		}
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see org.springframework.beans.factory.DisposableBean#destroy()
//	 */
//	@Override
//	public void destroy() throws Exception {
//
//	}
//
//}
