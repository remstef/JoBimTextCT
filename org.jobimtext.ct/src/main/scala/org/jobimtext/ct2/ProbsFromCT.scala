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

package org.jobimtext.ct2

import breeze.linalg.DenseVector
import breeze.numerics.log10
import org.apache.spark.rdd.RDD

/**
 * Created by Steffen Remus.
 */
object ProbsFromCT {

  val _df = 2

  /**
   * 2 degrees of freedom
   * @param lines_in (e1,e2,n11,n1dot,ndot1,n,ndocs)
   * @return (e1,e2,prob,log10prob)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {

    val probs = lines_in.map(line => line.split('\t'))
      .map( arr => (arr.take(_df).toList, DenseVector(arr.take(arr.length-1).takeRight(arr.length-1-_df).map(_.toDouble)), arr(arr.length-1)))
      .map({case (pair, ct2, ndocs) => (pair, getLog10ConditionalProbability(ct2))})

    val lines_out = probs.map({case (pair, log10prob) => pair.mkString("\t") + "\t%.3f\t%e".format(Math.pow(10,log10prob), log10prob) })

    return lines_out

  }

  def getLog10ConditionalProbability(ct2:DenseVector[Double]):Double = {
    val n11 = ct2(0)
    val n1dot = ct2(1)
    val log10prob = Math.log10(n11) - Math.log10(n1dot)
    return log10prob
  }

  def getLog10JointProbability(ct2:DenseVector[Double]):Double = {
    val n11 = ct2(0)
    val n = ct2(3)
    val log10prob = Math.log10(n11) - Math.log10(n)
    return log10prob
  }

}
