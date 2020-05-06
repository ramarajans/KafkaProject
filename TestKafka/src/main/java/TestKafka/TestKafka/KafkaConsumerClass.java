package TestKafka.TestKafka;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import junit.framework.Assert;

public class KafkaConsumerClass extends KafkaProducerClass{

	public static String topicName = null;
	public List<String> messageToValidate = new ArrayList<String>();
	public List<String> messagesReceived = new ArrayList<String>();

	KafkaConsumer consumer = null;
	int count=0,noRecodsCount=0;
	ConsumerRecords<String , String> records = null;
	boolean recordsFound = false;
	
	Properties prop = new Properties();
	Properties config = new Properties();
	
	FileInputStream inputStream = null;


	public static void main(String[] args) throws Exception{
		KafkaConsumerClass obj = new KafkaConsumerClass();
	}

	//Default Constructor
	public KafkaConsumerClass() throws Exception{
		super();
		topicName = super.topicName;
		messageToValidate = super.messageList;
		config = super.config;
		getMessage();
		validateMessage();
	}

	/*
	 * This method is to compare the messages sent from Producer and received by Consumer
	 * 
	 */
	public void validateMessage()throws Exception{
		
		//messagesReceived.add("test negative");
		
		System.out.println();
		if(messagesReceived.equals(messageToValidate))
			System.out.println("Messages sent by Producer and received by Consumer are same!!!");
		else
			System.out.println("Messages sent by Producer and received by Consumer are not matching!!!");
	}
	
	/*
	 * This method is to initialize the properties
	 * 
	 */
	public void initSettings()throws Exception{
		
		prop.put("bootstrap.servers", config.getProperty("bootstrap.servers"));
		prop.put("group.id", config.getProperty("group.id"));
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

	}
	
	/*
	 * This method is to consume the message sent by producer
	 * 
	 */
	public void getMessage()throws Exception{

		try{
			
			initSettings();
			
			consumer = new KafkaConsumer(prop);

			consumer.subscribe(Arrays.asList(topicName));
			System.out.println();
			System.out.println("Message received by Consumer:");
			
			try{
				while(true){
					records = consumer.poll(100);

					if(records.count()==0){
						noRecodsCount++;

						if(noRecodsCount>10)
							break;
						else
							continue;
					}

					for(ConsumerRecord<String, String> rec : records){
						System.out.println(rec.value());
						messagesReceived.add(rec.value());
						recordsFound = true;
					}

					consumer.commitAsync();

					if(recordsFound)
						break;
				}
				
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				consumer.close();
			}	

		}catch(KafkaException e){
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
