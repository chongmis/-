package cn.sheep.cms

import cn.sheep.utils.{JedisUtils, Utils}
import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
//import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.scalatest.time.Seconds
//重要的配置  改为  import scalikejdbc._
import scalikejdbc._
import scalikejdbc.config.DBs
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.log4j.{Level, Logger}


/**
  *思考我们现在要做什么，
  * 现在kafka中有数据,kafka正准备往里面写呢
  * 基本套路：
  *
  */
object RTMonitor {

  //屏蔽日志
  // 屏蔽日志
  Logger.getLogger("org.apache").setLevel(Level.WARN)


  def main(args: Array[String]): Unit = {

    val  load= ConfigFactory.load()

//    Streaming -kafka-0.8老版本的方法
  //创建kafka的相关参数  一个map类型
    val kafkaParams = Map(
      "metadata.broker.list" ->load.getString("kafka.broker.list"),
      "group.id" -> load.getString("kafka.group.id"),
      "auto.offset.reset" ->"smallest"
    )
    //同一个组可以到多个主题上消费数据
    val topics =load.getString("kafka.topics").split(",").toSet

    //StreamingContext上下文创建
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("实时统计")
    //conf   +  批次的时间
    val ssc = new StreamingContext(sparkConf, Seconds(2)) // 批次时间应该大于这个批次处理完的总的花费（total delay）时间
    //从kafka获取数据,-----从数据库中获取当前消费的偏移量（数据库还没有创建）位置，从该位置接着往后消费
//    val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition,Long]()

    // 加载配置信息
    DBs.setup()
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly{implicit session =>
       sql"select * from streaming_offset_24 where groupid=?".bind(load.getString("kafka.group.id")).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }.toMap


    //假设数据第一次启动  采用直连的方式
    /**
    Streaming -kafka-0.8老版本的方法
    val stream: Any = KafkaUtils.createDirectStream(ssc,KafkaParams,topic)
      */
      //kafka0.10的使用方式


    /*//假设程序第一次启动(判断条件表中没有数据)
      val stream: InputDStream[ConsumerRecord[String, String]] = if(fromOffsets.size == 0){
          KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topic,KafkaParams)//这里的topic（获取）  KafkaParams还是一个Map
          )
      }else{

        //拿到kafka的集群对象  0.8的版本吧
        val kafkaCluster = new KafkaCluster(kafkaParams);


        //程序非第一次启动  也就是我们程序之前存储一些偏移量    就是往偏移量之后的消费
//          KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe(topic,KafkaParams,fromOffsets)
        KafkaUtils.createDirectStream(ssc,
          LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, KafkaParams))
      }*/

      val stream = if (fromOffsets.size == 0) { // 假设程序第一次启动
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      } else {
        //提取kafka最新的偏移量  或者是老的偏移量
        var checkedOffset = Map[TopicAndPartition, Long]()
        //拿到kafka的集群对象
        val kafkaCluster = new KafkaCluster(kafkaParams)
        //拿到最早的偏移量 ERR  right
        val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)        //判断有没有right
        if (earliestLeaderOffsets.isRight) {
          //拿到Map
          val topicAndPartitionToOffset = earliestLeaderOffsets.right.get

          // 开始对比
          checkedOffset = fromOffsets.map(owner => {
            //集群上有的最早的有效的偏移量
            val clusterEarliestOffset = topicAndPartitionToOffset.get(owner._1).get.offset
            if (owner._2 >= clusterEarliestOffset) {
              owner
            } else {
              //偏移量过期
              (owner._1, clusterEarliestOffset)
            }
          })
        }
        // 程序菲第一次启动
        val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedOffset, messageHandler)
      }










    /**
      * receiver 接受数据是在Executor端 cache -- 如果使用的窗口函数的话，没必要进行cache, 默认就是cache， WAL （预写日志容错的）；
      *                                       如果采用的不是窗口函数操作的话，你可以cache, 数据会放做一个副本放到另外一台节点上做容错
      * direct 接受数据是在Driver端   SparkStreaming 整合kafka采用直连的方式是在Driver端执行的
      */
    //处理数据  ----根据业务需求
    stream.foreachRDD(rdd =>{
//      rdd.foreach(println)
      val offsetRanges: Array[OffsetRange]= rdd.asInstanceOf[HasOffsetRanges].offsetRanges


      //JSON->JsonObject  equalsIgnoreCase判断相等并且不区分大小写
      //获取充值成功的数据
      val baseData = rdd.map(t => JSON.parseObject(t._2))
        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(jsObj => {
          //获取充值成功的条目   ===0000
          val result = jsObj.getString("interFacRst")

          //获取充值的金额  必须是充值成功的金额 0000 显示指定Double才会发生转换
          val fee: Double = if (result.equals("0000")) jsObj.getDouble("chargefee") else 0
          //判断是否成功的
          val isSucc: Double = if (result.equals("0000")) 1 else 0

          //获取时间 先转换格式
          val reciverTime = jsObj.getString("receiveNotifyTime")
          val startTime: String = jsObj.getString("requestId")
          //公共类计算是时间差  消耗的时间  前提充值成功
          val costime: Double = if (result.equals("0000")) Utils.caculateRqt(startTime, reciverTime) else 0
          //获取省份
          val pCode = jsObj.getString("provinceCode")

          //计算成功的订单   充值成功率 (总的，是否充值成功，金额，消费时间)

          //把业务规划按照  key进行划分
          ("A-" + startTime.substring(0, 8),startTime.substring(0,10), List[Double](1, isSucc, fee, costime.toDouble),pCode,startTime.substring(0,12))
          //聚合操作
        })

      //实时报表
      /**
        * 充值金额 获取成功的金额
        * 统计全网的充值成功订单量,充值金额,充值成功率及充值平均时长  是当天的数据
        */
      baseData.map(t => (t._1,t._3)).reduceByKey((List1,List2) => {
        (List1 zip List2).map(x => x._1+x._2)//拉链操作
      }).foreachPartition(itr =>{//将数据存入到redis
          //获取jedis的连接
        val client = JedisUtils.getJedisClient()

        itr.foreach(tp =>{
          //流的方式  不断的递增
          //充值的总数量
          client.hincrBy(tp._1,"total",tp._2(0).toLong)
          //充值成功的总数量
          client.hincrBy(tp._1,"succ",tp._2(1).toLong)
          //充值成功的总金额
          client.hincrByFloat(tp._1,"money",tp._2(2))
          //充值成功的总时长
          client.hincrBy(tp._1,"timer",tp._2(3).toLong)

          //设置数据的有效期  为24小时  第二天时间就没有了  这里设置存储10天
          client.expire(tp._1,60 * 60 * 24 * 10)
        })
        //关闭连接
        client.close()

      })


      //每小时的数据分布情况 (小时，数组)
      baseData.map(t => ("B-"+t._2,t._3)).reduceByKey((List1,List2) => {
        (List1 zip List2).map(x => x._1+x._2)//拉链操作
      }).foreachPartition(itr =>{
        //获取jedis的连接
        val client = JedisUtils.getJedisClient()
        itr.foreach(tp =>{
          //流的方式  不断的递增
          //充值的总数量  //B-2017080512
          client.hincrBy(tp._1,"total",tp._2(0).toLong)
          //充值成功的总数量
          client.hincrBy(tp._1,"succ",tp._2(1).toLong)
          //succ/total 就是成功率
          //设置数据的有效期  为24小时  第二天时间就没有了  这里设置存储10天
          client.expire(tp._1,60 * 60 * 24 * 10)
        })
        //关闭连接
        client.close()
      })



      // 每个省份充值成功数据  ((时间，省份)，数组一些数据)
      baseData.map(t => ((t._2, t._4), t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map(x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          //获取（"p-日期(年月日)"，省份，充值成功的）  窜去到redis中
          client.hincrBy("P-"+tp._1._1.substring(0, 8), tp._1._2, tp._2(1).toLong)
          //设置数据的有效期  为24小时  第二天时间就没有了  这里设置存储10天
          client.expire("P-"+tp._1._1.substring(0, 8), 60 * 60 * 24 * 10)
        })
        client.close()
      })


      // 每分钟的数据分布情况统计
      baseData.map(t => ("C-"+t._5, t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map(x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          //充值成功的总数量  (分钟，“succ”,总数量)
          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
            //获取充值的金额
          client.hincrByFloat(tp._1, "money", tp._2(2))

          //设置数据的有效期  为24小时  第二天时间就没有了  这里设置存储10天
          client.expire(tp._1, 60 * 60 * 24 * 10)
        })
        client.close()
      })




      // 记录偏移量
      offsetRanges.foreach(osr => {
        DB.autoCommit{ implicit session =>
        sql"REPLACE INTO streaming_offset_24(topic, groupid, partitions, offset) VALUES(?,?,?,?)"
            .bind(osr.topic, load.getString("kafka.group.id"), osr.partition, osr.untilOffset).update().apply()
          //结果存入到redis  将偏移量存入到mysql
        }
        // println(s"${osr.topic} ${osr.partition} ${osr.fromOffset} ${osr.untilOffset}")
      })

    })


    // 每分钟的数据分布情况统计




    //启动程序   等待程序终止
    ssc.start()
    ssc.awaitTermination()

  }
}
