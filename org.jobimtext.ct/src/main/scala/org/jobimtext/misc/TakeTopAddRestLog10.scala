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

package org.jobimtext.misc

import org.apache.spark.rdd.RDD
import org.jobimtext.util.Util
import org.apache.spark.SparkContext._

/**
 * Created by Steffen Remus.
 */
object TakeTopAddRestLog10 {

  /**
   *
   * @param lines_in (u1,u2,log10prob)
   * @return (u1,u2,log10prob)
   */
  def apply(n:Int, lines_in:RDD[String]):RDD[String] = {

    val probs = lines_in.map(_.split("\t"))
      .map({case Array(u1,u2,prob,log10prob) => (u1, u2, log10prob.toDouble)})

    val top_probs = probs.map({case (u1, u2, log10prob) => (u1, (u2, log10prob)) })
      .groupByKey()  // TODO: improve performance by using reduceByKey and FixedSizeTreeSet (see TakeTopN)
      .map({case (u1, group) => (u1, takeTopMakeRest(n, u1, group.toSeq))})
      .flatMap({case (u1, group) => group.map({case (u2, log10prob) => (u1,u2,log10prob)})})

    val lines_out = top_probs.map({case (u1,u2,log10prob) =>  "%s\t%s\t%s".format(u1,u2,Util.format(log10prob))})

    return lines_out
  }

  def takeTopMakeRest(n:Int, u1:String, probs:Seq[(String, Double)]):Seq[(String, Double)] = {
//    var sum_all = probs.foldLeft(0d)((r,c) => r + Math.pow(10,c._2))
    var sum_all = probs.map(pair => Math.pow(10,pair._2)).sum
    var topn = probs.sortBy(-_._2).take(n)
//    val sum_topn = topn.foldLeft(0d)((r,c) => r + Math.pow(10,c._2))
    val sum_topn = topn.map(pair => Math.pow(10,pair._2)).sum
    val rest_prob = Math.log10(sum_all-sum_topn)
    topn = topn++Array(("__*_%s_*__".format(u1), rest_prob));

    return topn
  }

}