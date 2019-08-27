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

package swaydb.core.cache

import swaydb.IO

private[swaydb] object Lazy {
  def value[A](synchronised: Boolean,
               stored: Boolean,
               initial: Option[A]): LazyValue[A] = {
    val cache =
      new LazyValue[A](
        synchronised = synchronised,
        stored = stored
      )
    initial.foreach(value => cache.set(value))
    cache
  }

  def io[E: IO.ErrorHandler, A](synchronised: Boolean,
                             stored: Boolean,
                             initial: Option[A]): LazyIO[E, A] =
    new LazyIO[E, A](
      lazyValue =
        Lazy.value[IO.Right[E, A]](
          synchronised = synchronised,
          stored = stored,
          initial = initial.map(IO.Right(_))
        )
    )
}

protected sealed trait Lazy[A] {
  def get(): Option[A]
  def set(value: => A): A
  def getOrSet(value: => A): A
  def getOrElse[B >: A](f: => B): B
  def isDefined: Boolean
  def isEmpty: Boolean
  def clear(): Unit
}

private[swaydb] class LazyValue[A](synchronised: Boolean, stored: Boolean) extends Lazy[A] {

  @volatile private var cache: Option[A] = None

  override def get(): Option[A] =
    cache

  def set(value: => A): A =
    if (stored)
      if (synchronised) {
        this.synchronized {
          val got = value
          this.cache = Some(got)
          got
        }
      } else {
        val got = value
        cache = Some(got)
        got
      }
    else
      value

  def getOrSet(value: => A): A =
    cache getOrElse {
      if (synchronised)
        this.synchronized {
          cache.getOrElse {
            val got = value
            if (stored) cache = Some(got)
            got
          }
        }
      else {
        val got = value
        if (stored) cache = Some(got)
        got
      }
    }

  def getOrElse[B >: A](f: => B): B =
    get() getOrElse f

  def map[B](f: A => B): Option[B] =
    get() map f

  def flatMap[B >: A](f: A => Option[B]): Option[B] =
    get() flatMap f

  def isDefined: Boolean =
    get().isDefined

  def isEmpty: Boolean =
    get().isEmpty

  def clear(): Unit =
    this.cache = None
}

private[swaydb] class LazyIO[E: IO.ErrorHandler, A](lazyValue: LazyValue[IO.Right[E, A]]) extends Lazy[IO[E, A]] {

  def set(value: => IO[E, A]): IO[E, A] =
    try
      lazyValue set IO.Right(value.get)
    catch {
      case exception: Exception =>
        IO.failed[E, A](exception)
    }

  override def get(): Option[IO.Right[E, A]] =
    lazyValue.get()

  override def getOrSet(value: => IO[E, A]): IO[E, A] =
    try
      lazyValue getOrSet IO.Right(value.get)
    catch {
      case exception: Exception =>
        IO.failed[E, A](exception)
    }

  override def getOrElse[B >: IO[E, A]](f: => B): B =
    lazyValue getOrElse f

  def map[B](f: A => B): IO[E, Option[B]] =
    lazyValue
      .get()
      .map(_.map(f).map(Some(_)))
      .getOrElse(IO.none)

  def flatMap[B](f: A => IO[E, B]): IO[E, Option[B]] =
    lazyValue
      .get()
      .map(_.flatMap(f).map(Some(_)))
      .getOrElse(IO.none)

  override def isDefined: Boolean =
    lazyValue.isDefined

  override def isEmpty: Boolean =
    get().isEmpty

  override def clear(): Unit =
    lazyValue.clear()
}
