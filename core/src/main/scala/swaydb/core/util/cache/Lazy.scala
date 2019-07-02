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

package swaydb.core.util.cache

import swaydb.data.IO

object Lazy {
  def value[T](synchronised: Boolean): LazyValue[T] =
    new LazyValue[T](synchronised)

  def io[T](synchronised: Boolean): LazyIO[T] =
    new LazyIO[T](Lazy.value(synchronised))
}

protected sealed trait Lazy[V] {
  def get(): Option[V]
  def set(value: => V): V
  def getOrSet(value: => V): V
  def getOrElse[T >: V](f: => T): T
  def isDefined: Boolean
  def clear(): Unit
}

class LazyValue[V](isSynchronised: Boolean) extends Lazy[V] {

  @volatile private var cache: Option[V] = None

  override def get(): Option[V] =
    if (isSynchronised)
      this.synchronized(cache)
    else
      cache

  def set(value: => V): V = {
    val got = value
    if (isSynchronised)
      this.synchronized(cache = Some(got))
    else
      cache = Some(got)
    got
  }

  def map[T](f: V => T): Option[T] =
    get() map f

  def flatMap[T >: V](f: V => Option[T]): Option[T] =
    get() flatMap f

  def getOrSet(value: => V): V =
    cache getOrElse {
      val got = value
      if (isSynchronised)
        this.synchronized {
          cache.getOrElse {
            cache = Some(got)
            got
          }
        }
      else {
        cache = Some(got)
        got
      }
    }

  def getOrElse[T >: V](f: => T): T =
    cache getOrElse f

  def isDefined: Boolean =
    cache.isDefined

  def clear(): Unit =
    cache = None
}

class LazyIO[V](lazyValue: LazyValue[IO.Success[V]]) extends Lazy[IO[V]] {

  def set(value: => IO[V]): IO[V] = {
    val got = value
    got foreach {
      got =>
        lazyValue set IO.Success(got)
    }
    got
  }

  override def get(): Option[IO[V]] =
    lazyValue.get()

  override def getOrSet(value: => IO[V]): IO[V] =
    lazyValue getOrElse {
      value match {
        case success @ IO.Success(_) =>
          lazyValue set success
          success
        case failure @ IO.Failure(_) =>
          failure
      }
    }

  def map[T](f: V => T): IO[Option[T]] =
    lazyValue
      .get()
      .map(_.map(f).map(Some(_)))
      .getOrElse(IO.none)

  def flatMap[T](f: V => IO[T]): IO[Option[T]] =
    lazyValue
      .get()
      .map(_.flatMap(f).map(Some(_)))
      .getOrElse(IO.none)

  override def getOrElse[T >: IO[V]](f: => T): T =
    lazyValue getOrElse f

  override def isDefined: Boolean =
    lazyValue isDefined

  override def clear(): Unit =
    lazyValue clear()
}
