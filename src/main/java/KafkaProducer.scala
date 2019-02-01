import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io._;
import scala.io.Source;

object KafkaProducer {
  
  def main(args:Array[String]) {
    
    val topic:String = "testddosattack1"
    //val fis:FileInputStream = null
    //val br:BufferedReader = null
    
    val prop = new Properties();
		prop.put("bootstrap.servers","localhost:9092");
		prop.put("acks", "all");
		//prop.put("delivery.timeout.ms", 30000);
		prop.put("batch.size", "16384");
		//prop.put("linger.ms", 1);
		prop.put("buffer.memory","33554432");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		val producer = new org.apache.kafka.clients.producer.KafkaProducer[String,String](prop)
		
		val fileName = "C://phDataProject//apache-access-log.txt"
		//val fileName = "C://phDataProject//sample2.txt"
		var linecount =0 
		println("Start message publish")
		val bufferedSource = Source.fromFile(fileName)
		for(line <- bufferedSource.getLines()) {
		  linecount = linecount + 1
		  producer.send(new ProducerRecord[String, String](topic, Integer.toString(linecount),line))
		  //println(line)
		}
		
		
		println("Message publish completed")
		producer.close()
		bufferedSource.close()
		
    
  }
  
}