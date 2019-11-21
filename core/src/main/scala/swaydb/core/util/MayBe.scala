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

package swaydb.core.util

import swaydb.core.util.Tagged.@@

import scala.util.Try

sealed trait MayBeTag

object Maybe {

  type Maybe[T] = @@[T, MayBeTag]

  @inline def none[A]: Maybe[A] =
    null.asInstanceOf[Maybe[A]]

  @inline def some[V](value: V): Maybe[V] =
    value.asInstanceOf[Maybe[V]]

  implicit class ArrayImplicits[A](array: Array[A]) {
    final def findMaybe(p: A => Boolean): Maybe[A] = {
      var i = 0
      while (i < array.length - 1) {
        val item = array(i)
        if (p(item))
          return Maybe.some(item)
        else
          i += 1
      }

      Maybe.none[A]
    }
  }

  implicit class MaybeImplicits[A](value: Maybe[A]) {
    def isNone =
      value == null

    def isSome =
      value != null

    @inline def mapMayBe[B](f: A => B): Maybe[B] =
      if (value == null)
        none[B]
      else
        some(f(value))

    @inline def flatMapMayBe[B](f: A => Maybe[B]): Maybe[B] =
      if (value == null)
        none[B]
      else
        f(value)

    @inline def foreachMayBe(f: A => Unit): Unit =
      if (value != null)
        f(value)

    @inline def getUnsafe: A =
      if (value == null)
        throw new NoSuchElementException("MayBe.getUnsafe")
      else
        value

    @inline def orElseMayBe[B >: A](other: => Maybe[B]): Maybe[B] =
      if (value == null)
        other
      else
        value.asInstanceOf[Maybe[B]]

    @inline def applyOrElse[T](onSome: A => T, onNone: => T): T =
      if (value == null)
        onNone
      else
        onSome(value)

    @inline def getOrElseMayBe[B >: A](other: => B): B =
      if (value == null)
        other
      else
        value

    @inline def foldLeftMayBe[B](initial: B)(f: (A, B) => B): B =
      if (value == null)
        initial
      else
        f(value, initial)

    @inline def toOptionMayBe: Option[A] =
      Option(value)

    @inline def toTryMayBe: Try[A] =
      Try(value.getUnsafe)

  }
}
