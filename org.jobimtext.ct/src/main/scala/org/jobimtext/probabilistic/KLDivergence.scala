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
object KLDivergence {

  /**
   *
   * @param lines_in (e1,e2,prob,log10prob)
   * @return (e1,e1,KL)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {

    val probs = lines_in.map(_.split("\t"))
      .map({case Array(e1,e2,prob,log10prob) => (e1, e2, log10prob.toDouble)})

    // TODO: finish unfinished business

    return lines_in
  }

}
