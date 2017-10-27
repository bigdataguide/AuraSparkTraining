
package org.training.spark.datasource

import com.redislabs.provider.redis._
import org.training.spark.util.ExampleBase

object RedisExample extends ExampleBase {

  def main(args: Array[String]) {
    val redisServerDnsAddress = "bigdata"
    val redisPortNumber = 6379

    val stringRDD = sc.parallelize(Seq(("StringC", "StringD"), ("String3", "String4")))

    sc.toRedisKV(stringRDD, (redisServerDnsAddress, redisPortNumber))

  }

}
