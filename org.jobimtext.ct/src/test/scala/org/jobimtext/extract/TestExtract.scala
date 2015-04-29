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

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Steffen Remus.
 */
object TestExtract {

  def main(args: Array[String]) {
    testCooccurrenceWindowOnSpark(5)
    testCooccurrenceWindow(5)
    testCooccurrenceSentenceOnSpark
    testCooccurrenceSentence
  }

  def testCooccurrenceSentenceOnSpark() {
    val conf = new SparkConf()
      .setAppName("TestExtract")
      .setMaster("local[1]")
      .set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")

    val sc = new SparkContext(conf);

    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/samplesentences.txt").filter(_.nonEmpty)

    val lines_out = CooccurrenceWindow(-1, lines_in)
    lines_out.foreach(line => println(line));

    sc.stop()
  }

  def testCooccurrenceWindowOnSpark(windowsize:Int) {
    val conf = new SparkConf()
      .setAppName("TestExtract")
      .setMaster("local[1]")
      .set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")

    val sc = new SparkContext(conf);

//    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/samplesentences.txt").filter(_.nonEmpty)

    val lines_in = sc.parallelize(Seq("The quick brown fox jumps over the lazy dog"))

    val lines_out = CooccurrenceWindow(windowsize,lines_in)
    lines_out.foreach(line => println(line));

    sc.stop()
  }

  def testCooccurrenceSentence() {
    def x = CooccurrenceWindow.getCooccurrencesAll("1", "The quick brown fox jumps over the lazy dog".split(' '))
    x.foreach(println(_))
  }

  def testCooccurrenceWindow(windowsize:Int) {

    def x = CooccurrenceWindow.getCooccurrencesWindow("1", "The quick brown fox jumps over the lazy dog".split(' '), windowsize)
    x.foreach(println(_))

  }


}
