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

  def none: T

  def isNone: Boolean

  def get: SOME

  def toOption: Option[SOME] =
    if (isSome)
      Some(get)
    else
      None

  def isSome: Boolean =
    !isNone

  def mapSON[B](f: SOME => B): Option[B] =
    if (isSome)
      Some(f(get))
    else
      None

  def flatMapSON[B <: T](f: SOME => B): T =
    if (isSome)
      f(get)
    else
      none

  def flatMapSome[T2](none: T2)(f: SOME => T2): T2 =
    if (isSome)
      f(get)
    else
      none

  def flatMapOption[B](f: SOME => Option[B]): Option[B] =
    if (isSome)
      f(get)
    else
      None

  def foreachSON[B](f: SOME => B): Unit =
    if (isSome)
      f(get)

  def getOrElseSON[B <: SOME](other: => B): SOME =
    if (isSome)
      get
    else
      other

  def orElseSON[B <: T](other: => B): T =
    if (isSome)
      get
    else
      other

  def existsSON(f: SOME => Boolean): Boolean =
    isSome && f(get)

  def forallSON(f: SOME => Boolean): Boolean =
    isNone || f(get)

  def containsSON(f: SOME): Boolean =
    isSome && get == f

  def valueOrElse[B](f: SOME => B, orElse: B): B =
    if (isSome)
      f(get)
    else
      orElse

  def foldLeftSON[B](initial: B)(f: (B, SOME) => B): B =
    if (isSome)
      f(initial, get)
    else
      initial
}

private[swaydb] trait SomeOrNoneCovariant[+T, +SOME <: T] {

  def none: T

  def isNone: Boolean

  def get: SOME

  def toOptionSON: Option[SOME] =
    if (isSome)
      Some(get)
    else
      None

  def isSome: Boolean =
    !isNone

  def mapSON[B](f: SOME => B): Option[B] =
    if (isSome)
      Some(f(get))
    else
      None

  def flatMapSON[B >: T](f: SOME => B): T =
    if (isSome)
      f(get).asInstanceOf[T]
    else
      none

  def flatMapSome[T2](none: T2)(f: SOME => T2): T2 =
    if (isSome)
      f(get)
    else
      none

  def flatMapOption[B](f: SOME => Option[B]): Option[B] =
    if (isSome)
      f(get)
    else
      None

  def foreachSON[B](f: SOME => B): Unit =
    if (isSome)
      f(get)

  def getOrElseSON[B >: SOME](other: => B): SOME =
    if (isSome)
      get
    else
      other.asInstanceOf[SOME]

  def orElseSON[B >: T](other: => B): T =
    if (isSome)
      get
    else
      other.asInstanceOf[T]

  def valueOrElse[B](f: SOME => B, orElse: B): B =
    if (isSome)
      f(get)
    else
      orElse

  def existsSON(f: SOME => Boolean): Boolean =
    isSome && f(get)

  def forallSON(f: SOME => Boolean): Boolean =
    isNone || f(get)

  def containsSON[B >: SOME](f: B): Boolean =
    isSome && get == f

  def foldLeftSON[B](initial: B)(f: (B, SOME) => B): B =
    if (isSome)
      f(initial, get)
    else
      initial
}
