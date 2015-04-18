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

package org.jobimtext.sim

import org.apache.spark.rdd.RDD

/**
 * Created by Steffen Remus.
 */
object FreqSim {

  /**
   *
   * @param lines_in (u1,u2,f,freq1,freq2)
   * @return (u1,u2,freqsim)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {
    val joined_frequencies_shared_features = repr(lines_in)
    val freqsims = freqsim(joined_frequencies_shared_features)
    val lines_out = freqsims.map({case (u1,u2,freqsim) => "%s\t%s\t%d".format(u1,u2,freqsim)})
    return lines_out
  }

  def repr(lines_in:RDD[String]):RDD[(String, String, String, Long, Long)] = {
    return lines_in.map(_.split("\t"))
      .map({case Array(u1,u2,f,freq1,freq2) => (u1,u2,f,freq1.toDouble.toLong,freq2.toDouble.toLong)})
  }

  /**
   * compute similarity based on the number of shared features: freqsim(p,q) = sum_x p(x) && q(x)
   * @param data_in
   * @return
   */
  def freqsim(data_in:RDD[(String,String, String, Long, Long)]):RDD[(String, String, Long)] = {
    val freqsims = data_in
      .filter(t => t._4 > 0 && t._5 > 0) // remove zero overlap (should already be the case)
      .map({case (e1,e2,f,freq1,freq2) => ((e1,e2), 1l)})
      .reduceByKey(_+_) // (r,c) => (r+c)
      .map({case ((u1,u2), freqsim) => (u1,u2,freqsim)})
    return freqsims
  }

}
