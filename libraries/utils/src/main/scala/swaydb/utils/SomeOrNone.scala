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

private[swaydb] object SomeOrNone {

  implicit class OptionalImplicitsForSomeOrNone[A](option: Option[A]) {
    @inline final def flatMapOption[B](none: B)(f: A => B): B =
      if (option.isDefined)
        f(option.get)
      else
        none
  }
}

private[swaydb] trait SomeOrNone[T, SOME <: T] { selfS: T =>

  def noneS: T

  def isNoneS: Boolean

  def getS: SOME

  @inline def toOptionS: Option[SOME] =
    if (isSomeS)
      Some(getS)
    else
      None

  @inline def isSomeS: Boolean =
    !isNoneS

  @inline def mapS[B](f: SOME => B): Option[B] =
    if (isSomeS)
      Some(f(getS))
    else
      None

  @inline def mapOrElseS[B](empty: => B)(f: SOME => B): B =
    if (isSomeS)
      f(getS)
    else
      empty

  @inline def flatMapS[B <: T](f: SOME => B): T =
    if (isSomeS)
      f(getS)
    else
      noneS

  @inline def flatMapSomeS[T2](none: T2)(f: SOME => T2): T2 =
    if (isSomeS)
      f(getS)
    else
      none

  @inline def flatMapOptionS[B](f: SOME => Option[B]): Option[B] =
    if (isSomeS)
      f(getS)
    else
      None

  @inline def foreachS[B](f: SOME => B): Unit =
    if (isSomeS)
      f(getS)

  @inline def getOrElseS[B >: SOME](other: => B): SOME =
    if (isSomeS)
      getS
    else
      other.asInstanceOf[SOME]

  @inline def orElseS[B <: T](other: => B): T =
    if (isSomeS)
      selfS
    else
      other

  @inline def existsS(f: SOME => Boolean): Boolean =
    isSomeS && f(getS)

  @inline def forallS(f: SOME => Boolean): Boolean =
    isNoneS || f(getS)

  @inline def containsS(f: SOME): Boolean =
    isSomeS && getS == f

  @inline def valueOrElseS[B](f: SOME => B, orElse: B): B =
    if (isSomeS)
      f(getS)
    else
      orElse

  @inline def foldLeftS[B](initial: B)(f: (B, SOME) => B): B =
    if (isSomeS)
      f(initial, getS)
    else
      initial

  @inline def onSomeSideEffectS(f: SOME => Unit): T = {
    if (isSomeS)
      f(getS)

    selfS
  }

  @inline def onSideEffectS(f: T => Unit): T = {
    f(selfS)
    selfS
  }
}
