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

import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.FunctionUtil
import swaydb.data.config.BlockIO
import swaydb.data.{IO, Reserve}

import scala.util.Try

object Cache {

  def emptyValuesBlock: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    Cache.concurrentIO(synchronised = false, stored = true) {
      _ =>
        IO(ValuesBlock.emptyUnblocked)
    }

  def concurrentIO[I, O](synchronised: Boolean, stored: Boolean)(fetch: I => IO[O]): Cache[I, O] =
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

  def noIO[I, O](synchronised: Boolean, stored: Boolean)(fetch: I => O): CacheNOIO[I, O] =
    new CacheNOIO[I, O](
      fetch = fetch,
      lazyValue = Lazy.value(
        synchronised = synchronised,
        stored = stored
      )
    )

  def blockIO[I, O](blockIO: I => BlockIO, reserveError: => IO.Error.Busy)(fetch: I => IO[O]): Cache[I, O] =
    new BlockIOCache[I, O](
      Cache.noIO[I, Cache[I, O]](synchronised = false, stored = true) {
        i =>
          FunctionUtil.safe((_: I) => BlockIO.ConcurrentIO(false), blockIO)(i) match {
            case BlockIO.ConcurrentIO(cacheOnAccess) =>
              Cache.concurrentIO[I, O](
                synchronised = false,
                stored = cacheOnAccess
              )(fetch)

            case BlockIO.SynchronisedIO(cacheOnAccess) =>
              Cache.concurrentIO[I, O](
                synchronised = true,
                stored = cacheOnAccess
              )(fetch)

            case BlockIO.ReservedIO(cacheOnAccess) =>
              Cache.reservedIO[I, O](
                stored = cacheOnAccess,
                reserveError = reserveError
              )(fetch)
          }
      }
    )
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
sealed trait Cache[I, O] { self =>
  def value(i: => I): IO[O]
  def isCached: Boolean
  def clear(): Unit

  def getOrElse(f: => IO[O]): IO[O]

  def getSomeOrElse(f: => IO[Option[O]]): IO[Option[O]] =
    Try(getOrElse(???)).map(_.map(Some(_))) getOrElse f

  def map[O2](f: O => IO[O2]): Cache[I, O2] =
    new Cache[I, O2] {
      override def value(i: => I): IO[O2] = self.value(i).flatMap(f)
      override def isCached: Boolean = self.isCached
      override def getOrElse(f: => IO[O2]): IO[O2] = Try(value(???)).getOrElse(f)
      override def clear(): Unit = self.clear()
    }

  def mapStored[O2](f: O => IO[O2]): Cache[I, O2] =
    flatMap(Cache.concurrentIO(synchronised = false, stored = true)(f))

  def flatMap[O2](next: Cache[O, O2]): Cache[I, O2] =
    new Cache[I, O2] {
      //fetch the value from the lowest cache first. Higher caches should only be read if the lowest is not already computed.
      override def value(i: => I): IO[O2] = getOrElse(self.value(i).flatMap(next.value(_)))
      override def isCached: Boolean = self.isCached || next.isCached
      override def getOrElse(f: => IO[O2]): IO[O2] = next.getOrElse(f)
      override def clear(): Unit = {
        self.clear()
        next.clear()
      }
    }
}

private class BlockIOCache[I, O](cache: CacheNOIO[I, Cache[I, O]]) extends Cache[I, O] {

  override def value(i: => I): IO[O] =
    cache.value(i).value(i)

  override def isCached: Boolean =
    Try(cache.value(???).isCached).getOrElse(false)

  override def getOrElse(f: => IO[O]): IO[O] =
    IO(cache.value(???).value(???).get) recoverWith {
      case _ =>
        f
    }

  //clear the inner cache first, it unsuccessful then clear the outer cache.
  //why? outer cache is just an initialisation cache it does not do io/computation.
  //if it's called the second file
  override def clear(): Unit =
    Try(cache.value(???).clear()) getOrElse cache.clear()
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
class CacheNOIO[I, O](fetch: I => O, lazyValue: LazyValue[O]) {

  def value(input: => I): O =
    lazyValue getOrSet fetch(input)

  def isCached: Boolean =
    lazyValue.isDefined

  def getOrElse(f: => O): O =
    lazyValue getOrElse f

  def clear() =
    lazyValue.clear()
}
