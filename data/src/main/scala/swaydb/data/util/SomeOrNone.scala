/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.util

object SomeOrNone {

  implicit class OptionalImplicitsForSomeOrNone[A](option: Option[A]) {
    @inline def flatMapOption[B](none: B)(f: A => B): B =
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

  def toOptionS: Option[SOME] =
    if (isSomeS)
      Some(getS)
    else
      None

  def isSomeS: Boolean =
    !isNoneS

  def mapS[B](f: SOME => B): Option[B] =
    if (isSomeS)
      Some(f(getS))
    else
      None

  def flatMapS[B <: T](f: SOME => B): T =
    if (isSomeS)
      f(getS)
    else
      noneS

  def flatMapSomeS[T2](none: T2)(f: SOME => T2): T2 =
    if (isSomeS)
      f(getS)
    else
      none

  def flatMapOptionS[B](f: SOME => Option[B]): Option[B] =
    if (isSomeS)
      f(getS)
    else
      None

  def foreachS[B](f: SOME => B): Unit =
    if (isSomeS)
      f(getS)

  def getOrElseS[B >: SOME](other: => B): SOME =
    if (isSomeS)
      getS
    else
      other.asInstanceOf[SOME]

  def orElseS[B <: T](other: => B): T =
    if (isSomeS)
      selfS
    else
      other

  def existsS(f: SOME => Boolean): Boolean =
    isSomeS && f(getS)

  def forallS(f: SOME => Boolean): Boolean =
    isNoneS || f(getS)

  def containsS(f: SOME): Boolean =
    isSomeS && getS == f

  def valueOrElseS[B](f: SOME => B, orElse: B): B =
    if (isSomeS)
      f(getS)
    else
      orElse

  def foldLeftS[B](initial: B)(f: (B, SOME) => B): B =
    if (isSomeS)
      f(initial, getS)
    else
      initial

  def onSomeSideEffectS(f: SOME => Unit): T = {
    if (isSomeS)
      f(getS)

    selfS
  }

  def onSideEffectS(f: T => Unit): T = {
    f(selfS)
    selfS
  }
}

private[swaydb] trait SomeOrNoneCovariant[+T, +SOME <: T] { selfC: T =>

  def noneC: T

  def isNoneC: Boolean

  def getC: SOME

  def toOptionC: Option[SOME] =
    if (isSomeC)
      Some(getC)
    else
      None

  def isSomeC: Boolean =
    !isNoneC

  def mapC[B](f: SOME => B): Option[B] =
    if (isSomeC)
      Some(f(getC))
    else
      None

  def flatMapC[B >: T](f: SOME => B): T =
    if (isSomeC)
      f(getC).asInstanceOf[T]
    else
      noneC

  def flatMapSomeC[T2](none: T2)(f: SOME => T2): T2 =
    if (isSomeC)
      f(getC)
    else
      none

  def flatMapOptionC[B](f: SOME => Option[B]): Option[B] =
    if (isSomeC)
      f(getC)
    else
      None

  def foreachC[B](f: SOME => B): Unit =
    if (isSomeC)
      f(getC)

  def getOrElseC[B >: SOME](other: => B): SOME =
    if (isSomeC)
      getC
    else
      other.asInstanceOf[SOME]

  def orElseC[B >: T](other: => B): T =
    if (isSomeC)
      selfC
    else
      other.asInstanceOf[T]

  def valueOrElseC[B](f: SOME => B, orElse: B): B =
    if (isSomeC)
      f(getC)
    else
      orElse

  def existsC(f: SOME => Boolean): Boolean =
    isSomeC && f(getC)

  def forallC(f: SOME => Boolean): Boolean =
    isNoneC || f(getC)

  def containsC[B >: SOME](f: B): Boolean =
    isSomeC && getC == f

  def foldLeftC[B](initial: B)(f: (B, SOME) => B): B =
    if (isSomeC)
      f(initial, getC)
    else
      initial

  def onSomeSideEffectC(f: SOME => Unit): T = {
    if (isSomeC)
      f(getC)

    selfC
  }

  def onSideEffectC(f: T => Unit): T = {
    f(selfC)
    selfC
  }
}
