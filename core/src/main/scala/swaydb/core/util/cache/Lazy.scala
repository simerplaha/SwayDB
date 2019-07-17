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
  def value[T](synchronised: Boolean, stored: Boolean): LazyValue[T] =
    new LazyValue[T](
      synchronised = synchronised,
      stored = stored
    )

  def io[T](synchronised: Boolean, stored: Boolean): LazyIO[T] =
    new LazyIO[T](
      lazyValue = Lazy.value(
        synchronised = synchronised,
        stored = stored
      )
    )
}

protected sealed trait Lazy[V] {
  def get(): Option[V]
  def set(value: => V): V
  def getOrSet(value: => V): V
  def getOrElse[T >: V](f: => T): T
  def isDefined: Boolean
  def clear(): Unit
  def isStored: Boolean
  def isSynchronised: Boolean
}

class LazyValue[V](synchronised: Boolean, stored: Boolean) extends Lazy[V] {

  @volatile private var cache: Option[V] = None

  def isStored: Boolean = stored
  def isSynchronised: Boolean = synchronised

  override def get(): Option[V] =
    cache

  def set(value: => V): V =
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

  def getOrSet(value: => V): V =
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

  def getOrElse[T >: V](f: => T): T =
    get() getOrElse f

  def map[T](f: V => T): Option[T] =
    get() map f

  def flatMap[T >: V](f: V => Option[T]): Option[T] =
    get() flatMap f

  def isDefined: Boolean =
    get().isDefined

  def clear(): Unit =
    this.cache = None
}

class LazyIO[V](lazyValue: LazyValue[IO.Success[V]]) extends Lazy[IO[V]] {

  def isStored: Boolean = lazyValue.isStored
  def isSynchronised: Boolean = lazyValue.isSynchronised

  def set(value: => IO[V]): IO[V] =
    value map {
      value =>
        lazyValue set IO.Success(value)
        value
    }

  override def get(): Option[IO.Success[V]] =
    lazyValue.get()

  override def getOrSet(value: => IO[V]): IO[V] =
    IO {
      //meh! gotta ensure value is not fetched if set is not required.
      //need a better way.
      (lazyValue getOrSet IO.Success(value.get)).get
    }

  override def getOrElse[T >: IO[V]](f: => T): T =
    lazyValue getOrElse f

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

  override def isDefined: Boolean =
    lazyValue.isDefined

  override def clear(): Unit =
    lazyValue.clear()
}
