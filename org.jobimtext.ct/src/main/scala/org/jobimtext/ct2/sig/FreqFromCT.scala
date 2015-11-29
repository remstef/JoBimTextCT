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

import de.tudarmstadt.lt.scalautils.FormatUtils
import org.apache.spark.rdd.RDD
import org.jobimtext.ct2.CT2

/**
 * Created by Steffen Remus.
 */
object FreqFromCT {

  /**
   * 2 degrees of freedom
   * @param lines_in (ct2String)
   * @return (u1,u2,freq)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {

    val lines_out = lines_in.map(line => CT2.fromString(line))
      .map(ct2 => "%s\t%s\t%s".format(ct2.u1,ct2.u2,FormatUtils.format(ct2.n11)))

    return lines_out

  }

}
