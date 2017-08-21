package com.bohaisoft.dal.client.audit.message;//package com.yihaodian.ydal.client.audit.message;
//
//import java.util.Date;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.Logger;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//
//public class MessageSenderHelper {
//
//	private final static Logger logger = Logger.getLogger(MessageSenderHelper.class);
//	private static IMessageSender messageSender = null;
//	private static final int MAX_VALUE_LENGTH = 10000;
//	private static boolean isInit = false;
//	private static ThreadLocal<List<Map<String, Object>>> threadLocal = new ThreadLocal<List<Map<String, Object>>>() {
//		public List<Map<String, Object>> initialValue() {
//			return new LinkedList<Map<String, Object>>();
//		}
//	};
//
//	private static ThreadLocal<Map<String, Object>> threadMap = new ThreadLocal<Map<String, Object>>() {
//		public Map<String, Object> initialValue() {
//			return new HashMap<String, Object>();
//		}
//	};
//
//	private static synchronized void init() {
//		try {
//			ApplicationContext ctxt = new ClassPathXmlApplicationContext("classpath*:/com/yihaodian/ydal/client/audit/config/audit-beans.xml");
//			messageSender = (IMessageSender) ctxt.getBean("messageService");
//		} catch (Exception e) {
//			logger.error("Error while initializing audit message sender:" + e.getMessage());
//		}
//		isInit = true;
//	}
//
//	/**
//	 * 
//	 * @param key
//	 * @param value
//	 * @param type
//	 */
//	public static void add(Object value) {
//		add(value, null);
//	}
//
//	/**
//	 * 
//	 * @param key
//	 * @param value
//	 * @param type
//	 * @param map
//	 */
//	public static void add(Object value, Map<String, Object> props) {
//		Map<String, Object> map = new HashMap<String, Object>();
//		long millisecond = System.currentTimeMillis();
//		// map.put("key", key.getKey());
//		if (value == null)
//			value = "";
//		int splitNum = value.toString().length() > MAX_VALUE_LENGTH ? MAX_VALUE_LENGTH : value.toString().length();
//		map.put("value", value.toString().substring(0, splitNum));
//		// map.put("type", type.getType());
//		map.put("sendTime", new Date(millisecond));
//		map.put("sendTimeLong", millisecond);
//		Map<String, Object> tempMap = threadMap.get();
//		if (tempMap != null && tempMap.size() > 0) {
//			map.putAll(tempMap);
//		}
//
//		if (props != null && props.size() > 0) {
//			map.putAll(props);
//		}
//
//		threadLocal.get().add(map);
//	}
//
//	/**
//	 * 把容器里的缓存发送给jms
//	 */
//	public static void flush() {
//		if (!isInit) {
//			init();
//		}
//		if (messageSender == null) {
//			return;
//		}
//		try {
//			messageSender.send(threadLocal.get());
//			threadLocal = new ThreadLocal<List<Map<String, Object>>>() {
//				public List<Map<String, Object>> initialValue() {
//					return new LinkedList<Map<String, Object>>();
//				}
//			};
//		} catch (Exception e) {
//			logger.error("Error while sending audit message", e);
//		}
//	}
//
//	/**
//	 * 清除threadlocal中的缓存
//	 */
//	public static void remove() {
//		try {
//			if (threadLocal != null)
//				threadLocal.remove();
//			if (threadMap != null)
//				threadMap.remove();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//	}
//
//}
