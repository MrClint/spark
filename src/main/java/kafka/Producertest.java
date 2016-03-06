package kafka;

import java.util.Date;
import java.util.Properties;
import java.text.SimpleDateFormat;   

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

//消息生产
public class Producertest {

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("zk.connect", "master:2181,slave1:2181,slave2:2181,slave3:2181,slave3:2181,slave4:2181");
		// serializer.class为消息的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置metadata.broker.list, 为了高可用, 最好配两个broker实例
		props.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092,slave3:9092,slave4:9092");
		// 设置Partition类, 对队列进行合理的划分
		//props.put("partitioner.class", "idoall.testkafka.Partitionertest");
		// ACK机制, 消息发送需要kafka服务端确认
		props.put("request.required.acks", "1");

		props.put("num.partitions", "2");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		while(true)
		{

			int x=(int)(Math.random()*100);
			String ip = "202.109.201."+x;
			String text = "text"+x;
			String data = x+"|~|xxx|~|"+ip+"|~|xxx|~|xxx|~|xxx|~|";
			String  msg = ip+text+data;
			producer.send(new KeyedMessage<String, String>("m0127",null, msg));
			Thread.sleep(1000);
			System.out.println(msg);
		}
	}
	
}