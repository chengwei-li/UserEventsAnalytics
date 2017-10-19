package com.licw.demo

import scala.util.Random
import scala.util.Properties
//import org.apache.kafka.clients.producer.ProducerConfig
import kafka.javaapi.producer.Producer
import java.util.Properties

import net.sf.json.JSONObject


//import scala.util.parsing.json.JSONObject
import kafka.producer.ProducerConfig
import kafka.producer.KeyedMessage
/**
 * 模拟用户使用手机的行为事件
 * 
 *   uid：用户编号
 *   event_time：事件发生时间戳
 *   os_type：手机App操作系统类型
 *   click_count：点击次数
 * 
 */
object KafkaEventProducer {

  	private val users = Array(
  
  	"Tom", "Andy",
  
  	"Lily", "Jack",
  
  	"Lihua", "Tony",
  
  	"Hanmeimei", "Linda",
  
  	"Park", "James")
  
  	private val random = new Random()
  
  	private var pointer = -1
  
  	def getUserID() : String = {
  
  	pointer = pointer + 1
  
  	if(pointer >= users.length) {
  
  	pointer = 0
  
  	users(pointer)
  
  	} else {
  
  	users(pointer)
  
  	}
  
  	}
  
  	def click() : Double = {
  
  	random.nextInt(10)
  
  	}
     // 创建topic
  	// bin/kafka-topics.sh --zookeeper n1.troy.com:2181,n2.troy.com:2181,n3.troy.com:2181 --create --topic userclick --replication-factor 1 --partitions 2
    // 查看 topic列表
  	// bin/kafka-topics.sh --zookeeper n1.troy.com:2181,n2.troy.com:2181,n3.troy.com:2181 --list
    // 查看指定topic的明细
  	// bin/kafka-topics.sh --zookeeper n1.troy.com:2181,n2.troy.com:2181,n3.troy.com:2181 --describe userclick
    // 消费数据 查看
  	// bin/kafka-console-consumer.sh --zookeeper n1.troy.com:2181,n2.troy.com:2181,n3.troy.com:2181 --topic userclick --from-beginning
  
  	def main(args: Array[String]): Unit = {
  
  	val topic = "userclick"
  
  	val brokers = "n1.troy.com:6667"
  
  	val props = new Properties()
  
  	props.put("metadata.broker.list", brokers)
  
  	props.put("serializer.class", "kafka.serializer.StringEncoder")
  
  	val kafkaConfig = new ProducerConfig(props)
  
  	val producer = new Producer[String, String](kafkaConfig)
  
  	while(true) {
  
  	// prepare event data
  
  	val event = new JSONObject()
  
  	event.put("uid", getUserID)
  	event.put("event_time", System.currentTimeMillis.toString)
    event.put("os_type", "Android")
  	event.put("click_count", click)
  
  	// produce event message
  
  	producer.send(new KeyedMessage[String, String](topic, event.toString))
  
  	println("Message sent: " + event)
  
  	Thread.sleep(200)
  
  	}

	}

	}