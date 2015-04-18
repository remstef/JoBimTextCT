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

package org.jobimtext.extract

import org.apache.spark.rdd.RDD

/**
 * Created by Steffen Remus.
 */
object CooccurrenceSentence {

  def apply(lines_in:RDD[String]):RDD[String] = {
    val lines_out = lines_in.flatMap(getCooccurrences(_))
    return lines_out
  }

  def getCooccurrences(line:String):IndexedSeq[String] = {
    val tokens = line.split(' ')
    val id = Integer.toHexString(line.hashCode)

    for(i <- 0 until tokens.length; j <- i+1 until tokens.length)
      yield "%s\t%s\t%s\n%2$s\t%1$s\t%3$s".format(tokens(i), tokens(j), id)
  }

}
