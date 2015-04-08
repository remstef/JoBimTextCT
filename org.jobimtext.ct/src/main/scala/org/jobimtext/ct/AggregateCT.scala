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

/**
 * Created by Steffen Remus.
 */

import breeze.linalg.{DenseVector, sum}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object AggregateCT {

  /**
   *
   * @param df degrees of freedom
   * @param lines_in
   * @return
   */
   def apply(df:Int, lines_in:RDD[String]):RDD[String] = {
    //       .map({case Array(docid, e1, e2, n11, n12, n21, n22) => ((e1,e2), DenseVector(n11.toInt, n12.toInt, n21.toInt, n22.toInt, 1))})
     val lines_out = lines_in.map(line => line.split('\t'))
      .map(arr => ((arr.takeRight(arr.length-1).take(df).toList) , DenseVector(arr.takeRight(arr.length - df)++Array(1)) ) )
      .foreach(println _) //TODO: finish unfinished business
    null
   }

 }