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

package org.jobimtext

import breeze.linalg.{sum, DenseMatrix, Matrix, DenseVector}

import scala.util.Try

/**
 * Created by Steffen Remus.
 */
object TestRunner {

  def main(args: Array[String]) {
    val t = ("a", "b", 1, 2, 3, 4)
    val f = "%s\t"*t.productArity
    print(f)

    val s = Array("1","2",3,4,5)
    println(s.take(s.length-1).takeRight(s.length - 2).toList)
    val r = DenseVector(s.take(2)++Array(-1))
    println(r)

    println("-----")

    var m = DenseMatrix(Array("1","2","1","3").map(_.toDouble))
    println(m)
    m = m.reshape(2,2)
    println(m)
    println(sum(m(::,0)))

    println(m(0,1))

    println(m.reshape(1,4))

    println("%.0f".format(1d))

    val x = Try(0/0)
    println(x.get)

  }

}
