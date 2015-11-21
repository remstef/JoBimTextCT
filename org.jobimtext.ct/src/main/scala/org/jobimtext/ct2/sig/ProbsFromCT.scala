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

package org.jobimtext.ct2.sig

import org.apache.spark.rdd.RDD
import org.jobimtext.ct2.CT2
import org.jobimtext.util.Util

import scala.math.log10

/**
 * Created by Steffen Remus.
 */
object ProbsFromCT {

  /**
   * 2 degrees of freedom
   * @param lines_in (ct2String)
   * @return (u1,u2,log10prob)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {

    val probs = lines_in.map(line => CT2.fromString(line))
      .map(ct2 => (ct2, getLog10ConditionalProbability(ct2)))
      .map(tupl => "%s\t%s\t%s".format(tupl._1.u1, tupl._1.u2, Util.format(tupl._2)))
    val lines_out = probs

    return lines_out

  }

  /**
   *
   * p(u2|u1) = n11/n1dot => \sum_u2 p(u2|u1) = 1
   *
   * @param ct2
   * @return
   */
  def getLog10ConditionalProbability[T](ct2:CT2[T]):Double = {
    val log10prob = log10(ct2.n11) - log10(ct2.n1dot)
    return log10prob
  }

  /**
   *
   * p(u1,u2)
   *
   * @param ct2
   * @return
   */
  def getLog10JointProbability[T](ct2:CT2[T]):Double = {
    val log10prob = log10(ct2.n11) - log10(ct2.n)
    return log10prob
  }

}
