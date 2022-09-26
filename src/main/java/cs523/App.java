package cs523;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;  
import org.apache.spark.SparkConf;  
import org.apache.spark.streaming.Durations;  
import org.apache.spark.streaming.api.java.JavaInputDStream;  
import org.apache.spark.streaming.api.java.JavaStreamingContext;  
import org.apache.spark.streaming.kafka010.ConsumerStrategies;  
import org.apache.spark.streaming.kafka010.KafkaUtils;  
import org.apache.spark.streaming.kafka010.LocationStrategies;  
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {  
	final static Logger logger = LoggerFactory.getLogger(App.class);
	
	private static final String KAFKA_BROKER_LIST = "localhost:9092";
	private static final int STREAM_SECONDS = 5;
	private static final String APP_NAME = "DsSalaryStreamApp";
	private static final Collection<String> TOPICS = Arrays.asList("test-topic");
	
	private static final int SALARY_THRESHOLD = 150000;
	
	private static HbaseConnectionUtil hbaseUtil;
	
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException, IOException {
		
		// Spark configuration
		SparkConf sparkConf = new SparkConf().setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setAppName(APP_NAME);  
        
		// Kafka properties
		Map<String, Object> kafkaParams = new HashMap<>();  
		kafkaParams.put("bootstrap.servers", KAFKA_BROKER_LIST);  
		kafkaParams.put("key.deserializer", StringDeserializer.class);  
		kafkaParams.put("value.deserializer", StringDeserializer.class);  
		kafkaParams.put("group.id", "DEFAULT_GROUP_ID");  
		kafkaParams.put("auto.offset.reset", "latest");  
		kafkaParams.put("enable.auto.commit", false);
		
		// Hbase connection
		hbaseUtil = new HbaseConnectionUtil();
		
		try(JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(STREAM_SECONDS))) {  
		
			JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
					ssc, 
					LocationStrategies.PreferConsistent(), 
					ConsumerStrategies.<String, String>Subscribe(TOPICS, kafkaParams)
					);
			
			stream.foreachRDD(rdd -> {
				List<DsSalariesDTO> dsData = rdd.map(ConsumerRecord::value) // getting values
												.map(StrToDtoParser::parse) // parsing to DsSalariesDTO object
												.filter(f -> Integer.parseInt(f.getSalaryInUsd()) >= SALARY_THRESHOLD) // filtering by salary
												.collect();
				hbaseUtil.saveDataToHbase(dsData);
//		        rdd.coalesce(1).saveAsTextFile("/home/cloudera/workspace/BdtFinalProject/output");
		    });
			ssc.start();  
			ssc.awaitTermination();
		}
	}  	
	
	public static class StrToDtoParser {
        public static DsSalariesDTO parse(String s) {
            final String[] data = s.trim().split(",");
            
            DsSalariesDTO dto = new DsSalariesDTO();
            dto.setId(data[0].trim());
            dto.setWorkYear(data[1].trim());
            dto.setExperienceLevel(data[2].trim());
            dto.setJobTitle(data[4].trim());
            dto.setSalary(data[5].trim());
            dto.setSalaryCurrency(data[6].trim());
            dto.setSalaryInUsd(data[7].trim());
            dto.setCompanySize(data[11].trim());
            
            return dto;
        }
    }
	
}  
