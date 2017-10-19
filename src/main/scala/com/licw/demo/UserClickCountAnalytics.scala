package com.licw.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.kafka.common.serialization.StringDeserializer
import net.sf.json.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

/**
 * 模拟分析用户使用手机行为
 * 手机客户端会收集用户的行为事件（我们以点击事件为例），将数据发送到数据服务器，我们假设这里直接进入到Kafka消息队列
 * 后端的实时服务会从Kafka消费数据，将数据读出来并进行实时分析，这里选择Spark Streaming，因为Spark Streaming提供了与Kafka整合的内置支持
 * 经过Spark Streaming实时计算程序分析，将结果写入Redis，可以实时获取用户的行为数据，并可以导出进行离线综合统计分析
 * 
 */

 object UserClickCountAnalytics {
  
  	def main(args: Array[String]): Unit = {
  
  	var masterUrl = "local[2]"
  
  	if (args.length > 0) {
  
  	masterUrl = args(0)
  
  	}
  
  	
  
  	// Create a StreamingContext with the given master URL
  
  	val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
  
  	val ssc = new StreamingContext(conf, Seconds(5))
  
  	// Kafka configurations
   //checkpint 用户保存状态信息
   ssc.checkpoint("hdfs://n1.troy.com:8020/lcwtest/SparkStreaming/Checkpoint_Data");
    val topics =Array("userclick");

	 val kafkaParams = Map[String,Object](
	     //用于初始化链接到kafka集群的地址
	     "bootstrap.servers"->"10.0.40.50:6667",
	     "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "userclick_group_id", 
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
	 );
  
  	val dbIndex = 1
  
  	val clickHashKey = "app::users::click"
  
  	
  
  	// Create a direct stream
  
  	val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics,kafkaParams));
  
  	
  
  	val events = kafkaStream.flatMap(line => {
  
  	val data = JSONObject.fromObject(line.value())
  
  	Some(data)
  
  	})
  
  
  	// Compute user click times
  
  	val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
  	userClicks.print();
  	
    userClicks.foreachRDD(rdd => {
    
      	rdd.foreachPartition(partitionOfRecords => {
      
      	partitionOfRecords.foreach(pair => {
      	  
      
        	val uid = pair._1
        
        	val clickCount = pair._2
        	print(uid +"=>"+clickCount)
        	//redis pool
        	object RedisUtil2 extends Serializable{
              @transient private var pool :JedisPool = null;
              def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                          maxTotal: Int, maxIdle: Int, minIdle: Int):Unit={
                makePool2(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle,true,false,1000);
              }
               def makePool2(redisHost: String, redisPort: Int, redisTimeout: Int,
                          maxTotal: Int, maxIdle: Int, minIdle: Int,testOnBorrow: Boolean,
                          testOnReturn: Boolean, maxWaitMillis: Long):Unit={
                if(pool == null){
                  val poolConfig= new GenericObjectPoolConfig();
                  poolConfig.setMaxIdle(maxIdle);
                  poolConfig.setMaxTotal(maxTotal);
                  poolConfig.setMinIdle(minIdle);
                  poolConfig.setTestOnBorrow(testOnBorrow);
                  poolConfig.setTestOnReturn(testOnReturn);
                  poolConfig.setMaxWaitMillis(maxWaitMillis);
                  pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout);
                  
                  val hook = new Thread{
                    override def run =pool.destroy() ;
                  }
                  sys.addShutdownHook(hook.run());
                }
               }
               def getPool : JedisPool={
                 assert(pool != null)
                 pool
               }
            }
        	//redis config
       	   val maxTotal = 200
           val maxIdle = 50
           val minIdle = 10
           val redisHost = "10.0.40.3"
           val redisPort = 6379
           val redisTimeout = 30000
          RedisUtil2.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle);
       	  val jedis = RedisUtil2.getPool.getResource
       	  //批处理
          val pipeline = jedis.pipelined()
         //pipeline.select(dbIndex)
          //用户，点击量存入redis 用于业务展示
        	pipeline.hincrBy(clickHashKey, uid, clickCount)
        
        	RedisUtil2.getPool.returnBrokenResource(jedis)
        
        	})
      
      	})
      
   })
  
      
    
  
  	ssc.start()
  
  	ssc.awaitTermination()

	}

	}