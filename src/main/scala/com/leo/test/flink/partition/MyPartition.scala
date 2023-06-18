package com.leo.test.flink.partition

import org.apache.flink.api.common.functions.Partitioner


/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 09:36
  */
class MyPartition extends Partitioner[Long]{
  override def partition(key:Long, nums:Int): Int = {
    if(key%2==0){
      0
    }else{
      1
    }
  }
}
