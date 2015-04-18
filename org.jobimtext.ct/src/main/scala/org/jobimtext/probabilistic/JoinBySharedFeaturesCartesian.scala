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

/**
 * Created by Steffen Remus.
 */
object JoinBySharedFeaturesCartesian {

  /**
   *
   * @param lines_in
   * @return
   */
  def apply(lines_in:RDD[String]):RDD[String] = {
    val data_in = repr(lines_in)
    val data_out = join_shared_features(data_in)
    val lines_out = data_out.map({case (e1,e2,f1,l1,l2) => "%s\t%s\t%s\t%e\t%e".format(e1,e2,f1,l1,l2)})
    return lines_out
  }

  def repr(lines_in:RDD[String]):RDD[(String, String, Double)] = {
    return lines_in.map(_.split("\t"))
      .map({case Array(e,f,prob,log10prob) => (e, f, log10prob.toDouble)})
  }

  /**
   *
   * @param data_in (e,f,log10prob)
   * @return (e1,e2,f,log10prob1,log10prob2)
   */
  def join_shared_features(data_in:RDD[(String, String, Double)]):RDD[(String,String, String, Double, Double)] = {
    val data_out = data_in.cartesian(data_in) // less efficient than groupbykey, but parallalizable
      .filter({case ((e1,f1,l1),(e2,f2,l2)) => f1 == f2 && e1 != e2})
      .coalesce(data_in.partitions.size) // TODO: this seems awkward, why is there no method to determine a good number of partitions
      .map({case ((e1,f1,l1),(e2,f2,l2)) => (e1,e2,f1,l1,l2)})
    return data_out
  }



}
