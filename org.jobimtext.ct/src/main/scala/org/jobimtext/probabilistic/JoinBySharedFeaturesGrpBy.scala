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

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.jobimtext.misc.SimSortTopN
import org.jobimtext.spark.SparkConfigured
import org.jobimtext.util.FixedSizeTreeSet

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
  def apply(prune:Int = -1, lines_in: RDD[String]): RDD[String] = {
    val data_in = repr(lines_in)
    var data_out:RDD[(String, String, String, Double, Double)] = null
    if(prune > 0)
      data_out = join_pruned_shared_features(data_in, prune)
    else
      data_out = join_shared_features(data_in)
    val lines_out = data_out.map({ case (e1, e2, f1, l1, l2) => "%s\t%s\t%s\t%e\t%e".format(e1, e2, f1, l1, l2) })
    return lines_out
  }

  def repr(lines_in: RDD[String]): RDD[(String, String, Double)] = {
    return lines_in.map(_.split("\t"))
      .map({ case Array(e, f, value) => (e, f, value.toDouble) })
  }

  /**
   *
   * @param data_in (e,f,value)
   * @return (f,e1,e2,value1,value2)
   */
  def join_pruned_shared_features(data_in: RDD[(String, String, Double)], prune:Int): RDD[(String, String, String, Double, Double)] = {
    val data_out = data_in.map({ case (e, f, value) => (f, (e, value, FixedSizeTreeSet.empty(TakeTopN.ord_rev, prune))) }) // TODO: move ordering somewhere else
      .map({case (f, (e, value, s)) => (f, (s+=((e, value))))})
      .reduceByKey((r,c) => r++=c)
      .map({ case (f, group) => join_shared_features_local(f, group)})
      .flatMap(x => x)
    return data_out
  }

  /**
   *
   * @param data_in (e,f,value)
   * @return (f,e1,e2,value1,value2)
   */
  def join_shared_features(data_in: RDD[(String, String, Double)]): RDD[(String, String, String, Double, Double)] = {
    val data_out = data_in.map({ case (e, f, value) => (f, (e, value)) })
      .groupByKey() /* (f, (e1, value), (e2,value), (e3, value), ... ) */
      .map({ case (f, group) => join_shared_features_local(f, group)})
      .flatMap(x => x)
    return data_out
  }

  /**
   *
   * @param group ((e1, value1),(e2,value2),(e3,value3),...)
   * @return ((e1,e2,value1,value2),(e1,e2,value1,value3),(e2,e3,value2,value3),...)
   */
  def join_shared_features_local(f:String, group: Iterable[(String, Double)]): Iterable[(String, String, String, Double, Double)] = {
    return group.flatMap({case (e1,l1) => group.filter(_._1 != e1).map({case (e2,l2) => (e1,e2,f,l1,l2)})})
  }

}
