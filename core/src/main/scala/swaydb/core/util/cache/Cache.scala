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

import swaydb.data.{IO, Reserve}

object Cache {
  def io[V](synchronised: Boolean, stored: Boolean)(fetch: => IO[V]): Cache[V] =
    if (synchronised)
      new SynchronisedIO[V](
        init = fetch,
        lazyIO = Lazy.io(synchronised = true, stored = stored)
      )
    else
      new ReservedIO(
        init = fetch,
        lazyIO = Lazy.io(synchronised = false, stored = stored),
        reserve = Reserve()
      )

  def value[I, V](synchronised: Boolean, stored: Boolean)(fetch: PartialFunction[I, V]): CacheFunctionOutput[I, V] =
    new CacheFunctionOutput[I, V](
      f = fetch,
      lazyValue = Lazy.value(
        synchronised = synchronised,
        stored = stored
      )
    )
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
sealed trait Cache[V] {
  def value: IO[V]
  def getOrElse(f: => IO[V]): IO[V]
  def isCached: Boolean
  def clear(): Unit
  def set(value: V): Unit

  def map[T](f: V => T): IO[T] =
    value map f

  def flatMap[T](f: V => IO[T]): IO[T] =
    value flatMap f
}

private class SynchronisedIO[V](init: => IO[V], lazyIO: LazyIO[V]) extends Cache[V] {

  override def value: IO[V] =
    lazyIO getOrSet init

  override def getOrElse(f: => IO[V]): IO[V] =
    lazyIO getOrElse f

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear(): Unit =
    lazyIO.clear()

  override def set(value: V): Unit =
    lazyIO set IO.Success(value)
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
private class ReservedIO[V](init: => IO[V], lazyIO: LazyIO[V], reserve: Reserve[Unit]) extends Cache[V] {

  override def value: IO[V] =
    lazyIO.getOrElse {
      if (Reserve.setBusyOrGet((), reserve).isEmpty)
        try
          lazyIO set init
        finally
          Reserve.setFree(reserve)
      else
        IO.Failure(IO.Error.ReservedValue(reserve))
    }

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear() =
    lazyIO.clear()

  override def getOrElse(f: => IO[V]): IO[V] =
    lazyIO getOrElse f

  override def set(value: V): Unit =
    lazyIO set IO.Success(value)
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
class CacheFunctionOutput[I, V](f: I => V, lazyValue: LazyValue[V]) {

  def value(input: I): V =
    lazyValue getOrSet f(input)

  def isDefined: Boolean =
    lazyValue.isDefined

  def clear() =
    lazyValue.clear()
}
