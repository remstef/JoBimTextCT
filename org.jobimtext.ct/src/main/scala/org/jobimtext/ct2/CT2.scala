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

package org.jobimtext.ct2


import org.jobimtext.util.Util

/**
 * Created by Steffen Remus.
 */
object CT2 {

  val EMPTY_CT = CT2[String]("","",0f,0f,0f,0f,0f,0f,0f,0)

  def fromString(ct2AsString:String):CT2[String] = {
    ct2AsString.split("\t") match {
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22,ndocs) => CT2(u1,u2,n11.toFloat,n12.toFloat,n21.toFloat,n22.toFloat,o12.toFloat,o21.toFloat,o22.toFloat,ndocs.toInt)
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22) => CT2(u1,u2,n11.toFloat,n12.toFloat,n21.toFloat,n22.toFloat,o12.toFloat,o21.toFloat,o22.toFloat,1)
      case _ => EMPTY_CT
    }
  }

  def fromStringArray(ct2AsStringArray:Array[String]):CT2[String] = {
    ct2AsStringArray match {
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22,ndocs) => CT2(u1,u2,n11.toFloat,n12.toFloat,n21.toFloat,n22.toFloat,o12.toFloat,o21.toFloat,o22.toFloat, ndocs.toInt)
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22) => CT2(u1,u2,n11.toFloat,n12.toFloat,n21.toFloat,n22.toFloat,o12.toFloat,o21.toFloat,o22.toFloat,1)
      case _ => EMPTY_CT
    }
  }

}


case class CT2[T](u1:T, u2:T, var n11:Float, var n12:Float, var n21:Float, var n22:Float, var o12:Float, var o21:Float, var o22:Float, var ndocs:Int) {

  def n1dot() = n11 + n12
  def ndot1() = n11 + n21
  def n2dot() = n21 + n22
  def ndot2() = n12 + n22
  def n() = n11 + n12 + n21 + n22

  def o11() = 1f
  def o1dot() = o11 + o12
  def odot1() = o11 + o21
  def o() = o11 + o12 + o21 + o22

  def +(b:CT2[T]):CT2[T] =
    CT2[T](u1,u2,
      n11+b.n11,
      n12+b.n12,
      n21+b.n21,
      n22+b.n22,
      o12+b.o12,
      o21+b.o21,
      o22+b.o22,
      ndocs+b.ndocs)

  def +=(ct2:CT2[T]):CT2[T] = {
    n11+=ct2.n11
    n12+=ct2.n12
    n21+=ct2.n21
    n22+=ct2.n22
    o12+=ct2.o12
    o21+=ct2.o21
    o22+=ct2.o22
    ndocs+=ndocs
    this
  }

  override def toString():String = f"${u1}\t${u1}\t${Util.format(n11)}\t${Util.format(n12)}\t${Util.format(n21)}\t${Util.format(n22)}\t${Util.format(o12)}\t${Util.format(o21)}\t${Util.format(o22)}\t${ndocs}%d"

}
