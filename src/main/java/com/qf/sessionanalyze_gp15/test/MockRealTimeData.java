package com.qf.sessionanalyze_gp15.test;

import com.qf.sessionanalyze_gp15.constant.Constants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MockRealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};  
	private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();
	
	private KafkaProducer<Integer, String> producer;
	
	public MockRealTimeData() {
		provinceCityMap.put("Jiangsu", new String[]{"Nanjing", "Suzhou"});
		provinceCityMap.put("Hubei", new String[]{"Wuhan", "Jingzhou"});
		provinceCityMap.put("Hunan", new String[]{"Changsha", "Xiangtan"});
		provinceCityMap.put("Henan", new String[]{"Zhengzhou", "Luoyang"});
		provinceCityMap.put("Hebei", new String[]{"Shijiazhuang", "Zhangjiakou"});
		createProducerConfig();
	}

	private KafkaProducer createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", Constants.KAFKA_METADATA_BROKER_LIST);
		producer =  new KafkaProducer(props);
		return producer;
	}
	public void run() {
		while(true) {	
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			// 数据格式为：timestamp province city userId adId
			String log = new Date().getTime() + " " + province + " " + city + " " 
					+ random.nextInt(1000) + " " + random.nextInt(10);  
			//producer.send(new KeyedMessage<Integer, String>("AdRealTimeLog", log));
			producer.send(new ProducerRecord<Integer, String>("AdRealTimeLog",log));
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData MessageProducer = new MockRealTimeData();
		MessageProducer.start();
	}
	
}
