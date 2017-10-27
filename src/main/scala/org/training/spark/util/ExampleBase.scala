
package org.training.spark.util

import org.apache.spark.{SparkConf, SparkContext}

trait ExampleBase {

  private val defaultConf: SparkConf = {
    new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName(this.getClass.getSimpleName)
  }

  private var _sc: SparkContext = _

  def sc: SparkContext = synchronized {
    if (_sc == null) {
      _sc = new SparkContext(defaultConf)
    }
    _sc
  }

}
