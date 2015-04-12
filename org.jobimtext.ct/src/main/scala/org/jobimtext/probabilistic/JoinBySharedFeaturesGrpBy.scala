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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.TraversableOnce

/**
  * Created by Steffen Remus.
  */
object JoinBySharedFeaturesGrpBy {

  /**
   *
   * @param lines_in
   * @return
   */
  def apply(lines_in: RDD[String]): RDD[String] = {
    val data_in = repr(lines_in)
    val data_out = join_shared_features(data_in)
    val lines_out = data_out.map({ case (e1, e2, f1, l1, l2) => "%s\t%s\t%s\t%e\t%e".format(e1, e2, f1, l1, l2) })
    return lines_out
  }

  def repr(lines_in: RDD[String]): RDD[(String, String, Double)] = {
    return lines_in.map(_.split("\t"))
      .map({ case Array(e, f, prob, log10prob) => (e, f, log10prob.toDouble) })
  }

  /**
   *
   * @param data_in (e,f,log10prob)
   * @return (f,e1,e2,log10prob1,log10prob2)
   */
  def join_shared_features(data_in: RDD[(String, String, Double)]): RDD[(String, String, String, Double, Double)] = {
    val data_out = data_in.map({ case (e, f, log10prob) => (f, (e, log10prob)) })
      .groupByKey() /* (f, (e1, log10prob), (e2,log10prob), (e3, log10prob), ... ) */
      .map({ case (f, group) => join_shared_features_local(f, group)})
      .flatMap(x => x)
    return data_out

//      .reduce((r,c) => r.union(c))
//      .flatMap({case rdd => rdd.se}
//      .treeReduce((r,c) => r.union(c))

//      .flatMap({ case (f, group) => group.map({ case (e1, e2, log10prob_1, log10prob_2) => (e1, e2, f, log10prob_1, log10prob_2) }) })


//    val temp = data_in.map({ case (e, f, log10prob) => (f, (e, log10prob)) })
//    val data_out = temp.cogroup(temp).flatMap({case (f, (g1, g2)) => join_shared_features_local_iterable(f,g1,g2)})
//      .flatMap()




  }


//  def join_shared_features_local_cogrp(f:String, g1: Iterable[(String, Double)],g2: Iterable[(String, Double)]): Iterable[(String, String, Double, Double)] = {
//    val x =
//      for ((e1, l1) <- g1; (e2, l2) <- g2)
//        yield (e1, e2, l1, l2)
//    return x.seq
//  }

  /**
   *
   * @param group ((e1, log10prob_1),(e2,log10prob_2),(e3,log10prob_3),...)
   * @return ((e1,e2,log10prob_1,log10prob_2),(e1,e2,log10prob_1,log10prob_3),(e2,e3,log10prob_2,log10prob_3),...)
   */
  def join_shared_features_local(f:String, group: Iterable[(String, Double)]): Iterable[(String, String, String, Double, Double)] = {



//    for((e1, l1) <- group)
//      for((e2,l2) <- group if e1 != e2)
//        yield (e1, e2, f, l1, l2)

    return group.flatMap({case (e1,l1) => group.filter(_._1 != e1).map({case (e2,l2) => (e1,e2,f,l1,l2)})})


  }

//  def x(e1: String, l1: Double, g:RDD[(String, Double)]): RDD[(String, String, Double, Double)] = {
//
//  }

}