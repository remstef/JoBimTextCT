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

package org.jobimtext.probabilistic

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

/**
 * Created by Steffen Remus.
 */
object TopProbs {

  /**
   *
   * @param lines_in (e1,e2,prob,log10prob)
   * @return (e1,e1,KL)
   */
  def apply(n:Int, lines_in:RDD[String]):RDD[String] = {

    val probs = lines_in.map(_.split("\t"))
      .map({case Array(e1,e2,prob,log10prob) => (e1, e2, log10prob.toDouble)})

    val top_probs = probs.map({case (e1, e2, log10prob) => (e1, (e2, log10prob)) })
      .groupByKey()
      .map({case (e1, group) => (e1, filterTopMakeRest(n, group.toSeq))})
      .flatMap({case (e1, group) => group.map({case (e2, log10prob) => (e1,e2,log10prob)})})

    val lines_out = top_probs.map({case (e1,e2,log10prob) => e1 +"\t"+ e2 +"\t%.3f\t%e".format(Math.pow(10,log10prob), log10prob)})

    return lines_out
  }

  def filterTopMakeRest(n:Int, probs:Seq[(String, Double)]):Seq[(String, Double)] = {
    var topn = probs.sortBy(-_._2).take(n)
    val sum_prob = topn.foldLeft(0d)((r,c) => r + Math.pow(10,c._2))
    val rest_prob = Math.log10(1d-sum_prob)
    topn = topn++Array(("__REST_PROB__", rest_prob));

    return topn
  }

}
