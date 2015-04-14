/*
 *
 *  Copyright 2015.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.jobimtext.run

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.jobimtext.extract.CooccurrenceWindow
import org.jobimtext.spark.SparkConfigured

/**
 * Created by Steffen Remus.
 */
object ExtractRunner extends SparkConfigured{



  def main(args: Array[String]) {
    run(args)
  }


  override def run(conf: SparkConf, args: Array[String]): Unit = {

    val ws = conf.getOption("windowsize").getOrElse({println("Setting 'windowsize' to '%d'.".format(3)); "3"}).toInt

    val in = conf.getOption("in").getOrElse(throw new IllegalStateException("Missing input path. Specify with '-in=<file-or-dir>'."))
    val out = conf.getOption("out").getOrElse(throw new IllegalStateException("Missing output path. Specify with '-out=<dir>'."))

    val sc = new SparkContext(conf.setAppName("JoBimTextCT"))

    extractCoocWindow(sc,in,out,ws)

    sc.stop()
  }

  /**
   *
   * @param sc
   * @param in
   * @param out
   * @param windowsize
   * @return
   */
  def extractCoocWindow(sc:SparkContext,
                        in:String,
                        out:String,
                        windowsize:Int = 3
                         ):RDD[String] = {

    val lines_in = sc.textFile(in).filter(_.nonEmpty)
    var lines_out = CooccurrenceWindow(windowsize,lines_in)
    lines_out.saveAsTextFile(out)
    return lines_out

  }


}
