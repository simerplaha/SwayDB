/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.utils

private[swaydb] trait SomeOrNoneCovariant[+T, +SOME <: T] { selfC: T =>

  def noneC: T

  def isNoneC: Boolean

  def getC: SOME

  @inline def toOptionC: Option[SOME] =
    if (isSomeC)
      Some(getC)
    else
      None

  @inline def isSomeC: Boolean =
    !isNoneC

  @inline def mapC[B](f: SOME => B): Option[B] =
    if (isSomeC)
      Some(f(getC))
    else
      None

  @inline def mapOrElseC[B](none: => B)(f: SOME => B): B =
    if (isSomeC)
      f(getC)
    else
      none

  @inline def flatMapC[B >: T](f: SOME => B): T =
    if (isSomeC)
      f(getC).asInstanceOf[T]
    else
      noneC

  @inline def flatMapSomeC[T2](none: T2)(f: SOME => T2): T2 =
    if (isSomeC)
      f(getC)
    else
      none

  @inline def flatMapOptionC[B](f: SOME => Option[B]): Option[B] =
    if (isSomeC)
      f(getC)
    else
      None

  @inline def foreachC[B](f: SOME => B): Unit =
    if (isSomeC)
      f(getC)

  @inline def getOrElseC[B >: SOME](other: => B): SOME =
    if (isSomeC)
      getC
    else
      other.asInstanceOf[SOME]

  @inline def orElseC[B >: T](other: => B): T =
    if (isSomeC)
      selfC
    else
      other.asInstanceOf[T]

  @inline def valueOrElseC[B](f: SOME => B, orElse: B): B =
    if (isSomeC)
      f(getC)
    else
      orElse

  @inline def existsC(f: SOME => Boolean): Boolean =
    isSomeC && f(getC)

  @inline def forallC(f: SOME => Boolean): Boolean =
    isNoneC || f(getC)

  @inline def containsC[B >: SOME](f: B): Boolean =
    isSomeC && getC == f

  @inline def foldLeftC[B](initial: B)(f: (B, SOME) => B): B =
    if (isSomeC)
      f(initial, getC)
    else
      initial

  @inline def onSomeSideEffectC(f: SOME => Unit): T = {
    if (isSomeC)
      f(getC)

    selfC
  }

  @inline def onSideEffectC(f: T => Unit): T = {
    f(selfC)
    selfC
  }
}
