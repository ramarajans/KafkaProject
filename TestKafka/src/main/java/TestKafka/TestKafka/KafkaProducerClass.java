package TestKafka.TestKafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class KafkaProducerClass {

	public static String topicName = null;
	public List<String> messageList = new ArrayList<String>();
	KafkaProducer producer = null;
	ProducerRecord<String, String> record = null;
	
	Properties prop = new Properties();
	Properties propTopic = new Properties();

	public Properties config = new Properties();
	
	FileInputStream inputStream = null;

	/*public static void main(String[] args) throws Exception{
		KafkaProducerClass obj = new KafkaProducerClass();
	}*/
	
	public KafkaProducerClass() throws Exception{
		sendMessage();
	}

	public void intiSettings()throws Exception{

		inputStream = new FileInputStream(System.getProperty("user.dir")+"\\config.properties");
		config.load(inputStream);
		
		prop.put("bootstrap.servers", config.getProperty("bootstrap.servers"));
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		topicName = config.getProperty("TopicName");
		topicName = createTopic(topicName);
		
		messageList = Arrays.asList(config.getProperty("messages").split("\\|"));
	}
	
	public void sendMessage()throws Exception{

		try{

			intiSettings();

			producer = new KafkaProducer(prop);

			System.out.println("Message to be sent from Producer:");
			for(int i=0;i<messageList.size();i++){
				System.out.println(messageList.get(i));
				producer.send(new ProducerRecord(topicName, messageList.get(i)));
			}
		}catch(ProducerFencedException e){
			e.printStackTrace();
		}catch(OutOfOrderSequenceException e){
			e.printStackTrace();
		}catch(AuthorizationException e){
			e.printStackTrace();
		}catch(KafkaException e){
			e.printStackTrace();
			producer.abortTransaction();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			producer.close();
		}
	}
	
	public String createTopic(String topicName) throws Exception{

		propTopic.put("bootstrap.servers", config.getProperty("bootstrap.servers"));
		propTopic.put("group.id", config.getProperty("group.id"));
		propTopic.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propTopic.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		if(getTopic(topicName)){
			System.out.println("Topic "+topicName +" already exists!!");
			System.out.println();
		}
		else{
			System.out.println("Creating Topic : "+topicName);
			
			AdminClient adminClient = AdminClient.create(prop);
			NewTopic newTopic = new NewTopic(topicName, 1, (short)1);

			List<NewTopic> newTopics = new ArrayList<NewTopic>();
			newTopics.add(newTopic);

			adminClient.createTopics(newTopics);
			adminClient.close();
		}
		
		return topicName;
	}

	public boolean getTopic(String topic)throws Exception{

		AdminClient adminClient = AdminClient.create(propTopic);

		ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
		listTopicsOptions.listInternal(true);
		
		System.out.println(adminClient.listTopics(listTopicsOptions).names().get());
		System.out.println();
		Set<String> topicsList =  adminClient.listTopics(listTopicsOptions).names().get();
		
		if(topicsList.contains(topic))
			return true;
		else
			return false;
	}
}