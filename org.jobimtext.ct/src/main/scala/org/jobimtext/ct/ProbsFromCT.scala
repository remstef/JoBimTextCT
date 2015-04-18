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

package org.jobimtext.ct


import org.apache.spark.rdd.RDD

/**
 * Created by Steffen Remus.
 */
object ProbsFromCT {

  /**
   *
   * @param df degrees of freedom for the Contingency Table
   * @param lines_in
   * @return
   */
  def apply(df:Int, lines_in:RDD[String]):RDD[String] = {

    val lines_out = lines_in.map(line => line.split('\t'))
      .map(arr =>  ( arr.take(df).toList , arr.takeRight(arr.length - df)) )
      // TODO: finish unfinished business

    return lines_out.map(_.toString())

  }

}
