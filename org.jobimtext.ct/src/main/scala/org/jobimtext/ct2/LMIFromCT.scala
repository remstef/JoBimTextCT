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

import org.apache.spark.rdd.RDD

import scala.math.{log10, pow, log, max}

/**
 * Created by Steffen Remus.
 */
object LMIFromCT {

  /**
   * 2 degrees of freedom
   * @param lines_in (u1,u2,n11,n1dot,ndot1,n,ndocs)
   * @return (u1,u2,lmi)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {

    val values = lines_in.map(line => CT2.fromString(line))
      .map(ct2 => (ct2, lmi(ct2)))
    val lines_out = values.map({case (ct2,lmi) => "%s\t%s\t%e".format(ct2.u1,ct2.u2,lmi) })

    return lines_out

  }

  /**
   *
   * lmi(u1,u2) = n11 * pmi(u1,u2)
   * pmi(u1,u2) = log(n11 / n1dot * ndot1)
   *
   * @param ct2
   * @return
   */
  def lmi(ct2:CT2):Double = {
    val pmi = log(ct2.n11) - (log(ct2.n1dot) + log(ct2.ndot1))
    val lmi = ct2.n11 * pmi
    return lmi
  }

}
