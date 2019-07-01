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

import swaydb.data.{IO, Reserve}

object CacheValue {
  def apply[T](fetch: => IO[T]): CacheValue[T] =
    new CacheValueNotReserved[T](fetch)

  def reserved[T](fetch: => IO[T]): CacheValue[T] =
    new CacheValueReserved(fetch, Reserve())

  def partial[I, T](fetch: PartialFunction[I, T]): CacheFunctionValue[I, T] =
    new CacheFunctionValue[I, T](fetch)
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
sealed trait CacheValue[T] {
  def value: IO[T]
  def getValueOrElse(f: => IO[T]): IO[T]
  def isCached: Boolean
  def clear(): Unit
  def set(value: T): Unit

  def map[X](f: T => X): IO[X] =
    value map f

  def flatMap[X](f: T => IO[X]): IO[X] =
    value flatMap f
}

private class CacheValueNotReserved[T](init: => IO[T]) extends CacheValue[T] {

  @volatile private var cacheValue: Option[IO[T]] = None

  override def value: IO[T] =
    cacheValue getOrElse {
      init map {
        success =>
          cacheValue = Some(IO.Success(success))
          success
      }
    }

  override def getValueOrElse(f: => IO[T]): IO[T] =
    cacheValue getOrElse f

  override def isCached: Boolean =
    cacheValue.isDefined

  override def clear(): Unit =
    cacheValue = None

  override def set(value: T): Unit =
    cacheValue = Some(IO.Success(value))
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
private class CacheValueReserved[T](init: => IO[T], reserve: Reserve[Unit]) extends CacheValue[T] {

  private val cacheValue = CacheValue[T](init)

  override def value: IO[T] =
    cacheValue.getValueOrElse {
      if (Reserve.setBusyOrGet((), reserve).isEmpty)
        try
          cacheValue.value
        finally
          Reserve.setFree(reserve)
      else
        IO.Failure(IO.Error.FetchingValue(reserve))
    }

  override def isCached: Boolean =
    cacheValue.isCached

  override def clear() =
    cacheValue.clear()

  override def getValueOrElse(f: => IO[T]): IO[T] =
    cacheValue getValueOrElse f

  override def set(value: T): Unit =
    cacheValue set value
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
class CacheFunctionValue[I, O](f: I => O) {

  private val cacheValue = CacheValue[Option[O]](IO.none)

  def value(input: I): IO[O] =
    cacheValue map {
      value =>
        value.getOrElse {
          val output = f(input)
          cacheValue.set(Some(output))
          output
        }
    }

  def isCached: Boolean =
    cacheValue.isCached

  def clear() =
    cacheValue.clear()
}
