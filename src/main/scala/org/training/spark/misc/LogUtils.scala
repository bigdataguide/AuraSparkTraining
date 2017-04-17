
package org.training.spark.misc

import java.io.{File, FileWriter}

import scala.util.Random

object LogUtils {

  private val pattern = """([\d]+),([\d]+),(.+)""".r

  private val statuses = Seq(200, 404, 503)
  private val urls = Seq("a.html", "b.html", "c.html", "d.html")

  def parseLine(line: String): Option[(Int, Int, String)] = {
    line match {
      case pattern(status, ms, url) =>
        Some((status.toInt, ms.toInt, url))
      case _ => None
    }
  }

  def genLine(status: Int, ms: Int, url: String): String = {
    s"$status,$ms,$url"
  }

  def genFakeLogs(n: Int): Unit = {
    val writer = new FileWriter(new File("data/access.log"))
    (1 to n).foreach(i => {
      val status = statuses(Random.nextInt(statuses.length))
      val ms = Random.nextInt(100)
      val url = urls(Random.nextInt(urls.length))
      val line = genLine(status, ms, url)
      writer.write(line + "\n")
    })
    writer.close()
  }

  def main(args: Array[String]) {
    genFakeLogs(1000)
  }

}
