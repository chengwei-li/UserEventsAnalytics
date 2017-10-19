#UserEventsAnalytics
Kafka+Spark Streaming+Redis实时计算整合实践 foreachRDD输出到redis
1.应用场景
分析用户使用手机App的行为，描述如下所示：

    手机客户端会收集用户的行为事件（我们以点击事件为例），将数据发送到数据服务器，我们假设这里直接进入到Kafka消息队列
    后端的实时服务会从Kafka消费数据，将数据读出来并进行实时分析，这里选择Spark Streaming，因为Spark Streaming提供了与Kafka整合的内置支持
    经过Spark Streaming实时计算程序分析，将结果写入Redis，可以实时获取用户的行为数据，并可以导出进行离线综合统计分析
2.使用Maven创建scala工程
开发工具：Eclipse，语言版本：Scala2.11.8，Spark2.0，JDK8  #可参考pom.xml
3.Kafka Producer模拟程序，用来模拟向Kafka实时写入用户行为的事件数据，数据是JSON格式，示例如下：
Message sent: {"uid":"Tom","event_time":"1508394256714","os_type":"Android","click_count":3}
/**
 * 模拟用户使用手机的行为事件
 * 
 *   uid：用户编号
 *   event_time：事件发生时间戳
 *   os_type：手机App操作系统类型
 *   click_count：点击次数
 * 
 */
object KafkaEventProducer {/**/}
4.Kafka+Spark Streaming+Redis 分析数据
 object UserClickCountAnalytics {/**/}
5.项目打包，如UserEventsAnalytics-0.0.1-SNAPSHOT.jar， 放入集群环境
这里测试放入opt目录：/opt/UserEventsAnalytics-0.0.1-SNAPSHOT.jar
由于需要将分析结果存入redis，spark2/libs目录没有 redis相关jar，需要手动添加。
依然放入/opt目录：jedis-2.9.0.jar，commons-pool2-2.4.2.jar
6.直接eclipse本地运行KafkaEventProducer模拟程序，向kafka生产数据
7.集群环境，进入 /spark2/ 执行如下命令：（务必注意必须是spark2.*的环境，因为程序代码是spark2.0编译）
[hdfs@n1 spark2]$ /bin/spark-submit --class com.licw.demo.UserClickCountAnalytics --jars /opt/jedis-2.9.0.jar,/opt/commons-pool2-2.4.2.jar /opt/UserEventsAnalytics-0.0.1-SNAPSHOT.jar   
##由于代码中已经设置setMaster("local[2]"),此处未进行master设置 ，可更加需求设置为client，yarn等
8.程序执行会5秒输出一次如下结果：
-------------------------------------------
Time: 1508378225000 ms
-------------------------------------------
(Linda,10)
(Tom,15)
(Andy,12)
(Tony,9)
(James,7)
(Lily,23)
(Park,1)
(Lihua,9)
(Hanmeimei,9)
(Jack,12)
9.查看redis结果
127.0.0.1:6379> type "app::users::click"
hash
127.0.0.1:6379> hgetall "app::users::click"   #代码中设置的key
 1) "Linda"
 2) "149"
 3) "Lihua"
 4) "155"
 5) "Tom"
 6) "129"
 7) "Hanmeimei"
 8) "126"
 9) "Andy"
10) "158"
11) "Jack"
12) "152"
13) "Tony"
14) "138"
15) "James"
16) "130"
17) "Lily"
18) "154"
19) "Park"
20) "120"
