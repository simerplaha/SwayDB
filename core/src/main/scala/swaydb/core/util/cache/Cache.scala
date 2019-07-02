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
  def io[T](synchronised: Boolean)(fetch: => IO[T]): Cache[T] =
    if (synchronised)
      new SynchronisedIO[T](fetch, Lazy.io(synchronised = true))
    else
      new ReservedIO(fetch, Lazy.io(synchronised = false), Reserve())

  def value[I, T](synchronised: Boolean)(fetch: PartialFunction[I, T]): CacheFunctionOutput[I, T] =
    new CacheFunctionOutput[I, T](fetch, Lazy.value(synchronised))
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
sealed trait Cache[T] {
  def value: IO[T]
  def getOrElse(f: => IO[T]): IO[T]
  def isCached: Boolean
  def clear(): Unit
  def set(value: T): Unit

  def map[X](f: T => X): IO[X] =
    value map f

  def flatMap[X](f: T => IO[X]): IO[X] =
    value flatMap f
}

private class SynchronisedIO[T](init: => IO[T], lazyIO: LazyIO[T]) extends Cache[T] {

  override def value: IO[T] =
    lazyIO getOrSet init

  override def getOrElse(f: => IO[T]): IO[T] =
    lazyIO getOrElse f

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear(): Unit =
    lazyIO.clear()

  override def set(value: T): Unit =
    lazyIO set IO.Success(value)
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
private class ReservedIO[T](init: => IO[T], lazyIO: LazyIO[T], reserve: Reserve[Unit]) extends Cache[T] {

  override def value: IO[T] =
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

  override def getOrElse(f: => IO[T]): IO[T] =
    lazyIO getOrElse f

  override def set(value: T): Unit =
    lazyIO set IO.Success(value)
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
class CacheFunctionOutput[I, O](f: I => O, lazyValue: LazyValue[O]) {

  def value(input: I): O =
    lazyValue getOrSet f(input)

  def isDefined: Boolean =
    lazyValue.isDefined

  def clear() =
    lazyValue.clear()
}
