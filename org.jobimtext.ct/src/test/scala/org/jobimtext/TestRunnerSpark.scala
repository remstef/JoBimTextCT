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

import org.apache.spark.{SparkContext, SparkConf}
import org.jobimtext.classic._
import org.jobimtext.ct._
import org.jobimtext.ct2._

/**
 * Created by Steffen Remus.
 */
object TestRunnerSpark {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SparkTestRunner")
      .setMaster("local[*]")
      .set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")

    val sc = new SparkContext(conf);

//    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/artificial-ct.txt").filter(_.nonEmpty)
//    val lines_out = AggregateContingencyTableDF2.classic(lines_in)

    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/artificial-jb.txt").filter(_.nonEmpty)

//    val lines_out = AggregateCT2(ClassicToCT(lines_in));
//    val lines_out = AggregateCT2.classic(ClassicToCT(lines_in));

//    val lines_out = AggregateCT(2, ClassicToCT(lines_in));
    val lines_out =
        ProbsFromCT2(
          CT2Marginals(
            AggregateCT2(
              ClassicToCT(lines_in))));

    //lines_out.saveAsTextFile("org.jobimtext.ct/local_data/testout");
    lines_out.sortBy(x => x).collect().foreach(line => println(line));

    sc.stop();

  }

}
