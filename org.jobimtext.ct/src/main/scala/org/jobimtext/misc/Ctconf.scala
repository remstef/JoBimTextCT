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

import org.jobimtext.ct2.CT2

/**
 * Created by Steffen Remus.
 *
 * from classic JoBimText:
 *
 * min_ndot1    == -f       (2)
 * min_n1dot    == -w       (2)
 * min_n11      == -wf      (2) [classic 0]
 * max_odot1    == -wpfmax  (1000)
 * min_odot1    == -wpfmin  (2)
 *
 * min_sig      == -s       (0)
 * topn_f       == -p       (1000)
 * topn_s       == -t       (200)
 * min_sim      == -ms      (2)
 *
 * exclusively for jbtct:
 *
 * min_docs     (1)
 *
 */
case class Ctconf(min_ndot1:Double = 2,
                         min_n1dot:Double = 2,
                         min_n11:Double = 2,
                         max_odot1:Double = 1000,
                         min_odot1:Double = 2,

                         min_sig:Double = 0,
                         topn_f:Int = 1000,
                         topn_s:Int = 200,
                         min_sim:Int = 2,

                         min_docs:Long = 1
                        ) {

  /**
   *
   * @param ct2
   * @return similar to scala filter predicate function: return true if the current ct should be kept, false if it should be filtered
   */
  def filterCT(ct2:CT2):Boolean =
    (ct2.ndot1 >= min_ndot1)  &&
    (ct2.n1dot >= min_n1dot)  &&
    (ct2.n11 >= min_n11)      &&
    (ct2.odot1 <= max_odot1)  &&
    (ct2.odot1 >= min_odot1)  &&
    (ct2.ndocs >= min_docs)

  def filterBySignificance(valuetriple:(String, String, Double)):Boolean = valuetriple match {
    case (u1, u2, value) => (value >= min_sig)
    case _ => false
  }

  def filterBySimilarityScore(valuetriple:(String, String, Double)):Boolean = valuetriple match {
    case (u1, u2, value) => (value >= min_sim)
    case _ => false
  }

}


object Ctconf {

  val noFilter = Ctconf(1,1,1,Double.MaxValue,1,-Double.MaxValue,Int.MaxValue,Int.MaxValue,-Int.MaxValue,1)

  val default = Ctconf(
    min_ndot1 = 2,
    min_n1dot = 2,
    min_n11 = 2,
    max_odot1 = 1000,
    min_odot1 = 2,
    min_docs = 1,
    min_sig = 0, // Double.NegativeInfinity
    topn_f = 1000,
    topn_s = 200,
    min_sim = 2
  )

}

