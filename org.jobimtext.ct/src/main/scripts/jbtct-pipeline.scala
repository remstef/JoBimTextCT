import org.jobimtext.extract.CooccurrenceWindow

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

/**
 *
 * Example Scala / Spark Shell Script to configure and execute a jbtct pipeline
 *
 * Execute:
 *
 *    spark-shell --jars <jbtct.jar-file>[,more-jars] [<spark-options>] -i <script.scala>
 *
 * Execute within spark Shell:
 *    $ scala> :load <jbtct-pipeline.scala>
 *
 * Example:
 *
 *    spark-shell --jars jbtct.jar -i jbtct-pipeline.scala
 *    spark-shell --master local[*] --conf spark.ui.port=4041  --num-executors 4 --jars jbtct.jar  -i jbtct-pipeline.scala
 *    spark-shell --master yarn --num-executors 100 --queue testing --jars jbtct.jar -i jbtct-pipeline.scala
 *
 * Notes:
 *
 *    - val sc (SparkContext) is defined through spark-shell
 *    - fill in required variables below:
 *      - in
 *      - out
 *    - adjust configuration
 *      - ctconf
 *    - comment and uncomment lines to perform certain operations
 *
 * Configurations:
 *
 *    min_ndot1    == -f       (2)
 *    min_n1dot    == -w       (2)
 *    min_n11      == -wf      (2) [classic 0]
 *    max_odot1    == -wpfmax  (1000)
 *    min_odot1    == -wpfmin  (2)
 *    min_docs                 (1)
 *
 *    min_sig      == -s       (0)
 *    topn_f       == -p       (1000)
 *    topn_s       == -t       (200)
 *    min_s        == -ms      (2)
 *
 */

try {

import org.jobimtext.{ct2, sim}
import org.jobimtext.misc._

val in = "src/test/files/artificial-jb.txt" //"input-dir-or-file"
val out = "local_data/out-artificial-jb.txt" //"output-dir"

val ctconf = Ctconf(
  min_ndot1 = 2,
  min_n1dot = 2,
  min_n11 = 2,
  max_odot1 = 1000,
  min_odot1 = 2,
  min_docs = 1,
  min_sig = 0,
  topn_f = 1000,
  topn_s = 200,
  min_sim = 2
)

// set the name of the app
sc.getConf.setAppName("jbtct")

// read non empty lines from input dir or file
val lines_in = sc.textFile(in).filter(_.nonEmpty)

// compute co-occurrences in default jbtct format, 'jo <tab> bim <tab> docid' ...
val coocs = CooccurrenceWindow(5,lines_in)

// ... or read co-occurrences from default jbtct format
//val coocs = sc.textFile(in).filter(_.nonEmpty)

// compute, save and peek into aggregated contingency tables
val cts = ct2.AggregateCT.classic(ct2.ClassicToCT(coocs))
cts.saveAsTextFile(out + "_1ct")
cts.takeSample(withReplacement = false, num = 10, seed = 42l).foreach(println(_))

// compute, save and peek into aggregated contingency tables
val ctsp = Prune.pruneCT(ctconf.filterCT, cts)
ctsp.saveAsTextFile(out + "_2ctp")
ctsp.takeSample(withReplacement = false, num = 10, seed = 42l).foreach(println(_))

// compute, prune, take top n, save and peek significance scores from contingency tables
var sgnfnc = ct2.sig.FreqFromCT(cts) // ct2.sig.ProbsFromCT(cts) // ct2.sig.LMIFromCT(ctsp) //
sgnfnc = Prune.pruneByValue(ctconf.filterBySignificance, sgnfnc)
sgnfnc = TakeTopN(n = ctconf.topn_f, descending = true, sortbykey = false, sgnfnc)
sgnfnc.saveAsTextFile(out + "_3lmi")
sgnfnc.takeSample(withReplacement = false, num = 10, seed = 42l).foreach(println(_))

// compute, save, and peek into joined units by shared features
val jnd = JoinBySharedFeaturesGrpBy(prune = -1, sgnfnc) // JoinBySharedFeaturesCartesian(sgnfnc)
jnd.saveAsTextFile(out + "_4js")
jnd.takeSample(withReplacement = false, num = 10, seed = 42l).foreach(println(_))

// compute, take top n, save and peek into similarities
var smlr = sim.FreqSim(jnd) // sim.KLDivergence(jnd)
smlr = Prune.pruneByValue(ctconf.filterBySimilarityScore, smlr)
smlr = TakeTopN(n = ctconf.topn_s, descending = true, sortbykey = true, smlr)
smlr.saveAsTextFile(out + "_5sim")

}catch{
  case e:Throwable =>
    e.printStackTrace()
    println("Something's wrong! (%s: %s)".format(e.getClass.getSimpleName, e.getMessage))
}