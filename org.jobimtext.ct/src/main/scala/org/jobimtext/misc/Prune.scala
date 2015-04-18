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

package org.jobimtext.misc

import org.apache.spark.rdd.RDD
import org.jobimtext.ct2.CT2
import org.jobimtext.util.Util

/**
 * Created by Steffen Remus.
 */
object Prune {

  def pruneCT(filterfun: CT2 => Boolean = (CT2) => true, lines_in:RDD[String]):RDD[String] = {

    val values = lines_in.map(line => CT2.fromString(line))
      .filter(filterfun)

    val lines_out = values.map(ct2 => ct2.toString())

    return lines_out

  }

  def pruneByValue(filterfun: ((String, String, Double)) => Boolean, lines_in:RDD[String]):RDD[String] = {
    lines_in.map(_.split("\t"))
      .map({ case Array(u1, u2, value) => (u1, u2, value.toDouble) })
      .filter(filterfun)
      .map({ case (u1,u2,value) => "%s\t%s\t%s".format(u1,u2, Util.format(value)) })
  }


}
