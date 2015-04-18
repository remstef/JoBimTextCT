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
import org.jobimtext.util.FixedSizeTreeSet
import org.apache.spark.SparkContext._

/**
 * Created by Steffen Remus.
 */
object SimSortTopN_deleteme {

  def apply( topn:Int = 200, descending:Boolean = false, lines_in:RDD[String]):RDD[String] = {

    val lines_out = lines_in.map(_.split("\t"))
      .map({case Array(o1,o2,sim) => (o1,(o2,sim.toDouble), FixedSizeTreeSet.empty(if (descending) TakeTopN.ord_rev else TakeTopN.ord,topn))})
      .map({case (o1,tupl,sortedset) => (o1, (sortedset+=(tupl)))})
      .reduceByKey((r,c) => (r++=(c)))
      .sortByKey()
      .flatMap({case(o1, s) => s.toSeq.map({case (o2,sim) => (o1,o2,sim)})})
      .map({case (o1,o2,sim) => "%s\t%s\t%f".format(o1,o2,sim)})
    return lines_out

  }

}
