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

/**
 * Created by Steffen Remus.
 */
object KLDivergenceGrpBy {

  /**
   *
   * @param lines_in (e,f,prob,log10prob)
   * @return (e,f,D_KL)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {

    val joined_probability_distributions_shared_features = repr(lines_in)
    val kl_divergence = kl(joined_probability_distributions_shared_features)
    val lines_out = kl_divergence.map({case (e,f,dkl) => "%s\t%s\t%f".format(e,f,dkl)})

    return lines_out
  }

  def repr(lines_in:RDD[String]):RDD[(String, String, String, Double, Double)] = {
    return lines_in.map(_.split("\t"))
      .map({case Array(e1,e2,f,log10prob1,log10prob2) => (e1,e2,f,log10prob1.toDouble,log10prob2.toDouble)})
  }

  def kl(data_in:RDD[(String,String, String, Double, Double)]):RDD[(String, String, Double)] = {
    val filtered_distributions = data_in.filter(t => !(t._4.isInfinite)) // remove zero P probabilities ( D_KL(P||Q) ) i.e. they sum to 0 anyway
      .map({case (e1,e2,f,log10prob_1,log10prob_2) => ((e1,e2),(log10prob_1, if(log10prob_2.isInfinite) -100 else log10prob_2))})
    val kl_divergences = filtered_distributions.groupByKey()
      .map({case ((e1,e2), group) => (e1,e2,kl_local(group.toSeq))})
    return kl_divergences
  }

  /**
   * compute Kullback Leibler Divergence: D_KL(p|q) = sum_x p(x)*log(p(x)/q(x))
   * @param data_in
   * @return
   */
  def kl_local(data_in:Seq[(Double, Double)]):Double = {
    var kl = data_in.map({case (log10prob_p, log10Prob_q) => Math.pow(10, log10prob_p) * (log10prob_p - log10Prob_q)}).sum
    /* account for negative D_KL values, which happens because P and Q do not sum to one. Zero values in q can be ignored, accounting for p is necessary, add small delta for q for that case */
    val sum_p = data_in.map({case (log10prob_p, log10Prob_q) => Math.pow(10, log10prob_p)}).sum
    if(sum_p < 1d)
      kl += (1d-sum_p) * (Math.log10(sum_p) + 100)
    return kl
  }

}
