/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.util

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
