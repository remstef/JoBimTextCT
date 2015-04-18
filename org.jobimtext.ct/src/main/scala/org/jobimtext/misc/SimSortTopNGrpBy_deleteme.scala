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
import org.apache.spark.SparkContext._
import org.jobimtext.util.Util

/**
 * Created by Steffen Remus.
 */
object SimSortTopNGrpBy_deleteme {


  def apply(lines_in:RDD[String], topn:Int = 200, reverse:Boolean = false):RDD[String] = {

    val lines_out = lines_in.map(_.split("\t"))
      .map({case Array(o1,o2,sim) => (o1,(o2,sim.toDouble))})
      .groupByKey()
      .flatMap({case (o1, group) => sort_local(group.toSeq, topn, reverse).map({case (o2,sim) => (o1, o2, sim)})})
      .sortBy(t => t._1)
      .map({case (o1,o2,sim) => "%s\t%s\t%s".format(o1,o2,Util.format(sim))})

    return lines_out
  }


  def sort_local(data_in:Seq[(String, Double)], topn:Int = 200, reverse:Boolean = false):Seq[(String,Double)] = {
    val sorted = if(reverse) data_in.sortBy({case (k,v) => -v}) else data_in.sortBy({case (k,v) => v})
    return sorted.take(topn)
  }

}
