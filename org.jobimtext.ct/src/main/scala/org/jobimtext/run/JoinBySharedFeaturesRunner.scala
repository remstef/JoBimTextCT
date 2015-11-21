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

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jobimtext.ct2
import org.jobimtext.misc.JoinBySharedFeaturesGrpBy
import org.jobimtext.spark.SparkConfigured
import org.jobimtext.util.FixedSizeTreeSet

/**
 * Created by Steffen Remus.
 */
object JoinBySharedFeaturesRunner extends SparkConfigured {

  def main(args: Array[String]) {
    run(args)
  }

  override def run(conf: SparkConf, args: Array[String]) {

    val in = conf.getOption("in").getOrElse(throw new IllegalStateException("Missing input path. Specify with '-in=<file-or-dir>'."))
    val out = conf.getOption("out").getOrElse(throw new IllegalStateException("Missing output path. Specify with '-out=<dir>'."))
    val prune = conf.getOption("prune").getOrElse({println("Setting 'prune' to '%d'.".format(-1)); "-1"}).toInt

    conf.setAppName("JoBimTextCT")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[FixedSizeTreeSet[_]], classOf[ct2.CT2[_]]))
    val sc = new SparkContext(conf)

    run(sc, in, out, prune)

    sc.stop()

  }

  def run(sc:SparkContext, in:String, out:String, prune:Int = -1):RDD[String] = {

    val lines_in = sc.textFile(in)
    val lines_out = JoinBySharedFeaturesGrpBy(prune, lines_in)
//    val lines_out = JoinBySharedFeaturesCartesian(lines_in)
    lines_out.saveAsTextFile(out)
    return lines_out

  }

}