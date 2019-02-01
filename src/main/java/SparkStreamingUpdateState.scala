import java.util.Properties

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import kafka.serializer._
//import kafka.utils._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka010._
import java.io._

object SparkStreamingUpdateState {
  
  def main(args:Array[String]) {
    
    val topic = "testddosattack1"
    val zookeeperServer = "localhost:2181"
    val kafkaBroker = "localhost:9092"
    
    val conf = new SparkConf().setAppName("sparkstreamingdemo").
                    setMaster("local[*]").
                    set("spark.streaming.stopGracefullyOnShutdown","true")
    
    val sc = new StreamingContext(conf,Seconds(2))
    
    sc.sparkContext.setLogLevel("ERROR")
    
    // checkpoint directory to maintain the state
    sc.checkpoint("C//phDataProject//checkpoint30jannew")
    
    val kafkaParams = Map[String, Object](
          "bootstrap.servers"            -> "localhost:9092",
          "key.deserializer"                -> classOf[StringDeserializer],
          "value.deserializer"              -> classOf[StringDeserializer],  
          "zookeeper.connect"               -> "localhost:2181",
          "group.id"                        -> "group-6",
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (true:java.lang.Boolean))
    
    
          
          
    val kafkaInputStream = KafkaUtils.createDirectStream[String,String](sc,PreferConsistent,Subscribe[String,String](Array("testddosattack1"),kafkaParams))
    
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)

    val previousCount = state.getOrElse(0)

    Some(currentCount + previousCount)
  }
    
    val TIME_IN_MS = System.currentTimeMillis().toString()
    val input = kafkaInputStream.map(stream => (stream.value.split(" ").map { e => e.trim() }.array(0), stream.value.split(" ").map { e => e.trim() }.array(3).substring(1)))
    val inputMap = input.map(line => (line,1)).updateStateByKey(updateFunc)
    val filterInputMap = inputMap.filter(tuple => (tuple._2 > 1)) 
    val ipMap = filterInputMap.map(tuple => tuple._1._1)

    ipMap.foreachRDD{inputRDD =>
      var newInputRDD = inputRDD.coalesce(1,true).distinct()  
      if(newInputRDD.count() > 0) {
        newInputRDD.saveAsTextFile("C://phDataProject//ddos_30jannew//output"+"-"+TIME_IN_MS)
      }
    
    }
    
    sc.start();
    //Thread.sleep(60);
    sc.awaitTermination();
    //sc.stop();
    
    
  }
  
  
}