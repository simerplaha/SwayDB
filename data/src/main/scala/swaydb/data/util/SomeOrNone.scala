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

private[swaydb] trait SomeOrNone[T, SOME <: T] {

  def noneSON: T

  def isNoneSON: Boolean

  def getSON: SOME

  def toOptionSON: Option[SOME] =
    if (isSomeSON)
      Some(getSON)
    else
      None

  def isSomeSON: Boolean =
    !isNoneSON

  def mapSON[B](f: SOME => B): Option[B] =
    if (isSomeSON)
      Some(f(getSON))
    else
      None

  def flatMapSON[B <: T](f: SOME => B): T =
    if (isSomeSON)
      f(getSON)
    else
      noneSON

  def flatMapSome[T2](none: T2)(f: SOME => T2): T2 =
    if (isSomeSON)
      f(getSON)
    else
      none

  def flatMapOption[B](f: SOME => Option[B]): Option[B] =
    if (isSomeSON)
      f(getSON)
    else
      None

  def foreachSON[B](f: SOME => B): Unit =
    if (isSomeSON)
      f(getSON)

  def getOrElseSON[B <: SOME](other: => B): SOME =
    if (isSomeSON)
      getSON
    else
      other

  def orElseSON[B <: T](other: => B): T =
    if (isSomeSON)
      getSON
    else
      other

  def existsSON(f: SOME => Boolean): Boolean =
    isSomeSON && f(getSON)

  def forallSON(f: SOME => Boolean): Boolean =
    isNoneSON || f(getSON)

  def containsSON(f: SOME): Boolean =
    isSomeSON && getSON == f

  def valueOrElseSON[B](f: SOME => B, orElse: B): B =
    if (isSomeSON)
      f(getSON)
    else
      orElse

  def foldLeftSON[B](initial: B)(f: (B, SOME) => B): B =
    if (isSomeSON)
      f(initial, getSON)
    else
      initial
}

private[swaydb] trait SomeOrNoneCovariant[+T, +SOME <: T] {

  def noneSONC: T

  def isNoneSONC: Boolean

  def getSONC: SOME

  def toOptionSONC: Option[SOME] =
    if (isSomeSONC)
      Some(getSONC)
    else
      None

  def isSomeSONC: Boolean =
    !isNoneSONC

  def mapSONC[B](f: SOME => B): Option[B] =
    if (isSomeSONC)
      Some(f(getSONC))
    else
      None

  def flatMapSONC[B >: T](f: SOME => B): T =
    if (isSomeSONC)
      f(getSONC).asInstanceOf[T]
    else
      noneSONC

  def flatMapSomeSONC[T2](none: T2)(f: SOME => T2): T2 =
    if (isSomeSONC)
      f(getSONC)
    else
      none

  def flatMapOptionSONC[B](f: SOME => Option[B]): Option[B] =
    if (isSomeSONC)
      f(getSONC)
    else
      None

  def foreachSONC[B](f: SOME => B): Unit =
    if (isSomeSONC)
      f(getSONC)

  def getOrElseSONC[B >: SOME](other: => B): SOME =
    if (isSomeSONC)
      getSONC
    else
      other.asInstanceOf[SOME]

  def orElseSONC[B >: T](other: => B): T =
    if (isSomeSONC)
      getSONC
    else
      other.asInstanceOf[T]

  def valueOrElseSONC[B](f: SOME => B, orElse: B): B =
    if (isSomeSONC)
      f(getSONC)
    else
      orElse

  def existsSONC(f: SOME => Boolean): Boolean =
    isSomeSONC && f(getSONC)

  def forallSONC(f: SOME => Boolean): Boolean =
    isNoneSONC || f(getSONC)

  def containsSONC[B >: SOME](f: B): Boolean =
    isSomeSONC && getSONC == f

  def foldLeftSONC[B](initial: B)(f: (B, SOME) => B): B =
    if (isSomeSONC)
      f(initial, getSONC)
    else
      initial
}
