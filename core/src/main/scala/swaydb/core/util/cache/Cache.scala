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

import scala.util.Try

object Cache {

  def io[I, O](synchronised: Boolean, reserved: Boolean, stored: Boolean)(fetch: I => IO[O]): Cache[I, O] =
    if (synchronised || !reserved)
      syncAsyncIO(
        synchronised = synchronised,
        stored = stored
      )(fetch)
    else
      reservedIO(
        stored = stored,
        reserveError = IO.Error.ReservedValue(Reserve())
      )(fetch)

  def syncAsyncIO[I, O](synchronised: Boolean, stored: Boolean)(fetch: I => IO[O]): Cache[I, O] =
    new SynchronisedIO[I, O](
      fetch = fetch,
      lazyIO = Lazy.io(synchronised = synchronised, stored = stored)
    )

  def reservedIO[I, O](stored: Boolean, reserveError: IO.Error.Busy)(fetch: I => IO[O]): Cache[I, O] =
    new ReservedIO(
      fetch = fetch,
      lazyIO = Lazy.io(synchronised = false, stored = stored),
      error = reserveError
    )

  def unsafe[I, O](synchronised: Boolean, stored: Boolean)(f: I => O): CacheUnsafe[I, O] =
    new CacheUnsafe[I, O](
      f = f,
      lazyValue = Lazy.value(
        synchronised = synchronised,
        stored = stored
      )
    )

  def io[I, O](synchronised: I => Boolean, reserved: I => Boolean, stored: I => Boolean)(fetch: I => IO[O]): Cache[I, O] =
    new DelayedCache[I, O](
      Cache.unsafe[I, Cache[I, O]](synchronised = false, stored = true) {
        i =>
          Cache.io[I, O](
            synchronised = Try(synchronised(i)).getOrElse(false),
            reserved = Try(reserved(i)).getOrElse(false),
            stored = Try(stored(i)).getOrElse(true)
          )(fetch)
      }
    )
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
sealed trait Cache[I, O] {
  def value(i: => I): IO[O]
  def isCached: Boolean
  def clear(): Unit

  def getOrElse(f: => IO[O]): IO[O]

  def map[T](i: I)(f: O => T): IO[T] =
    value(i) map f

  def foreach[T](i: I)(f: O => T): Unit =
    value(i) foreach f

  def flatMap[T](i: I)(f: O => IO[T]): IO[T] =
    value(i) flatMap f
}

private class DelayedCache[I, O](cache: CacheUnsafe[I, Cache[I, O]]) extends Cache[I, O] {

  override def value(i: => I): IO[O] =
    cache.value(i).value(i)

  override def isCached: Boolean =
    cache.isCached && Try(cache.value(???).isCached).getOrElse(false)

  override def getOrElse(f: => IO[O]): IO[O] =
    Try(cache.value(???).value(???)) getOrElse f

  override def clear(): Unit =
    cache.clear()
}

private class SynchronisedIO[I, O](fetch: I => IO[O],
                                   lazyIO: LazyIO[O]) extends Cache[I, O] {

  override def value(i: => I): IO[O] =
    lazyIO getOrSet fetch(i)

  override def getOrElse(f: => IO[O]): IO[O] =
    lazyIO getOrElse f

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear(): Unit =
    lazyIO.clear()
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
private class ReservedIO[I, O](fetch: I => IO[O], lazyIO: LazyIO[O], error: IO.Error.Busy) extends Cache[I, O] {

  override def value(i: => I): IO[O] =
    lazyIO getOrElse {
      if (Reserve.setBusyOrGet((), error.reserve).isEmpty)
        try
          lazyIO set fetch(i)
        finally
          Reserve.setFree(error.reserve)
      else
        IO.Failure(error)
    }

  override def isCached: Boolean =
    lazyIO.isDefined

  override def getOrElse(f: => IO[O]): IO[O] =
    lazyIO getOrElse f

  override def clear() =
    lazyIO.clear()
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
class CacheUnsafe[I, O](f: I => O, lazyValue: LazyValue[O]) {

  def value(input: => I): O =
    lazyValue getOrSet f(input)

  def isCached: Boolean =
    lazyValue.isDefined

  def clear() =
    lazyValue.clear()
}
