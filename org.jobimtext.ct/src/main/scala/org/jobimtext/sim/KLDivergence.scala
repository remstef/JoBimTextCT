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
import org.apache.spark.SparkContext._
import org.jobimtext.util.Util

/**
 * Created by Steffen Remus.
 */
object KLDivergence {

  /**
   *
   * @param lines_in (e1,e2,f,log10prob1,log10prob2)
   * @return (e,f,D_KL)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {
    val joined_probability_distributions_shared_features = repr(lines_in)
    val kl_divergence = kl(joined_probability_distributions_shared_features)
    val lines_out = kl_divergence.map({case (e,f,dkl) => "%s\t%s\t%s".format(e,f,Util.format(dkl))})
    return lines_out
  }

  def repr(lines_in:RDD[String]):RDD[(String, String, String, Double, Double)] = {
    return lines_in.map(_.split("\t"))
      .map({case Array(e1,e2,f,log10prob1,log10prob2) => (e1,e2,f,log10prob1.toDouble,log10prob2.toDouble)})
  }

  /**
   * compute Kullback Leibler Divergence: D_KL(p|q) = sum_x p(x)*log(p(x)/q(x))
   * @param data_in
   * @return
   */
  def kl(data_in:RDD[(String,String, String, Double, Double)]):RDD[(String, String, Double)] = {
    val filtered_distributions = data_in.filter(t => !(t._4.isInfinite)) // remove zero P probabilities ( D_KL(P||Q) ) i.e. they sum to 0 anyway
      .map({case (e1,e2,f,log10prob_1,log10prob_2) => ((e1,e2),(log10prob_1, if(log10prob_2.isInfinite) -100 else log10prob_2))})
    val kl_divergences = filtered_distributions
      .map({case ((e1,e2),(lp, lq)) => ((e1,e2),(Math.pow(10, lp) * (lp - lq), Math.pow(10, lp)))}) // left: kl_sum_term, right p_x_sum_term
      .reduceByKey((r,c) => (r._1+c._1, r._2+c._2))
      .map({case ((e1,e2),(kl_sum, p_x_sum)) => (e1,e2, if(p_x_sum < 1d) kl_sum+(1d-p_x_sum) * (Math.log10(p_x_sum) + 100) else kl_sum)}) // repair kl divergence, w.r.t. sum p_x
    return kl_divergences
  }


}
