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

package org.jobimtext.util

/**
 * Created by Steffen Remus.
 */
object FixedSizeTreeSetTest {

  def main(args: Array[String]) {

    val ord = new Ordering[(String, Double)] {
      def compare(o1:(String, Double), o2:(String, Double)): Int = {
        val r = o1._2.compareTo(o2._2)
        if(r == 0)
          o1._1.compareTo(o2._1)
        r
      }
    }

    val s = FixedSizeTreeSet.empty(ord,3)
//    s.maxsize = 3
    s += (("hello", 1d))
    s += (("hello", 2d))
    s += (("hello", 3d))
    s += (("hello", 4d))
    s ++= Seq(("hello", 3.5d),("hello", 2.5d),("hello", 1.5d))


    s.foreach(println(_))


    val t = new FixedSizeTreeSet[Int](5)
    t ++= Seq(1,2,5,4,3,2,55,2,1,2,4,2,3,41,1,24,4,3,1,23)
    t += 5
    t += 0
    t+=23
    t ++= Seq(5,0,23)

    println(t.maxsize)
    println(s.maxsize)
    println(t)
    t.foreach(println(_))


  }

}
