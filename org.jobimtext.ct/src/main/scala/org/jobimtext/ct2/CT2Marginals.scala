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

import breeze.linalg.{sum, DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD

/**
 * Created by Steffen Remus.
 */
object CT2Marginals {

  val _df = 2

  /**
   * 2 degrees of freedom
   * @param lines_in (e1,e2,n11,n12,n21,n22,ndocs)
   * @return (e1,e2,n11,n1dot,ndot1,ndocs)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {

    val ct_marginal_sums = lines_in.map(line => line.split('\t'))
      .map( arr => (arr.take(_df).toList, DenseVector(arr.take(arr.length-1).takeRight(arr.length-1-_df).map(_.toInt)), arr(arr.length-1)))
      .map({case (pair, ct2, ndocs) => (pair, getMarginalSums(ct2), ndocs)})
    val lines_out = ct_marginal_sums
      .map({case (pair, ct2, ndocs) => pair.mkString("\t") + "\t" + ct2.toArray.mkString("\t") + "\t" + ndocs })
    return lines_out

  }

  def getMarginalSums(ct2:DenseVector[Int]):DenseVector[Int] = {
    val ct2_margins = ct2.copy
    ct2_margins(0) = ct2(0)
    ct2_margins(1) = ct2(0) + ct2(1)
    ct2_margins(2) = ct2(0) + ct2(2)
    ct2_margins(3) = sum(ct2)
    return ct2_margins
  }

}
