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

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.FunctionUtil
import swaydb.data.Reserve
import swaydb.data.config.IOStrategy
import swaydb.data.io.Core
import swaydb.{ErrorHandler, IO}

private[core] object Cache {

  def valueIO[E: ErrorHandler, I, O](output: O): Cache[E, I, O] =
    new Cache[E, I, O] {
      override def value(i: => I): IO[E, O] = IO(output)
      override def isCached: Boolean = true
      override def clear(): Unit = ()
      override def get(): Option[IO.Success[E, O]] = Option(IO.Success(output))
      override def getOrElse(f: => IO[E, O]): IO[E, O] = IO(output)
    }

  def emptyValuesBlock[E: ErrorHandler]: Cache[E, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    Cache.concurrentIO(synchronised = false, stored = true) {
      _ =>
        IO(ValuesBlock.emptyUnblocked)
    }

  def concurrentIO[E: ErrorHandler, I, O](synchronised: Boolean, stored: Boolean)(fetch: I => IO[E, O]): Cache[E, I, O] =
    new SynchronisedIO[E, I, O](
      fetch = fetch,
      lazyIO = Lazy.io(synchronised = synchronised, stored = stored)
    )

  def reservedIO[E: ErrorHandler, ER <: E with Core.Error.Reserved, I, O](stored: Boolean,
                                                                          reserveError: ER)(fetch: I => IO[E, O]): Cache[E, I, O] =
    new ReservedIO[E, ER, I, O](
      fetch = fetch,
      lazyIO = Lazy.io(synchronised = false, stored = stored),
      error = reserveError
    )

  def noIO[I, O](synchronised: Boolean, stored: Boolean)(fetch: I => O): NoIO[I, O] =
    new NoIO[I, O](
      fetch = fetch,
      lazyValue = Lazy.value(
        synchronised = synchronised,
        stored = stored
      )
    )

  def blockIO[E: ErrorHandler, ER <: E with Core.Error.Reserved, I, O](blockIO: I => IOStrategy,
                                                                       reserveError: => ER)(fetch: I => IO[E, O]): Cache[E, I, O] =
    new BlockIOCache[E, I, O](
      Cache.noIO[I, Cache[E, I, O]](synchronised = false, stored = true) {
        i =>
          FunctionUtil.safe((_: I) => IOStrategy.ConcurrentIO(false), blockIO)(i) match {
            case IOStrategy.ConcurrentIO(cacheOnAccess) =>
              Cache.concurrentIO[E, I, O](
                synchronised = false,
                stored = cacheOnAccess
              )(fetch)

            case IOStrategy.SynchronisedIO(cacheOnAccess) =>
              Cache.concurrentIO[E, I, O](
                synchronised = true,
                stored = cacheOnAccess
              )(fetch)

            case IOStrategy.ReservedIO(cacheOnAccess) =>
              Cache.reservedIO[E, ER, I, O](
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
private[core] sealed abstract class Cache[E: ErrorHandler, I, O] extends LazyLogging { self =>
  def value(i: => I): IO[E, O]
  def isCached: Boolean
  def clear(): Unit

  def get(): Option[IO.Success[E, O]]

  def getOrElse(f: => IO[E, O]): IO[E, O]

  def getSomeOrElse(f: => IO[E, Option[O]]): IO[E, Option[O]] =
    get().map(_.map(Some(_))) getOrElse f

  /**
   * An adapter function that applies the map function to the input on each invocation.
   * The result does not get stored in this cache.
   *
   * [[mapStored]] Or [[flatMap]] functions are used for where storage is required.
   */
  def map[O2](f: O => IO[E, O2]): Cache[E, I, O2] =
    new Cache[E, I, O2] {
      override def value(i: => I): IO[E, O2] = self.value(i).flatMap(f)
      override def isCached: Boolean = self.isCached
      override def getOrElse(f: => IO[E, O2]): IO[E, O2] = get() getOrElse f
      override def get(): Option[IO.Success[E, O2]] =
        self.get() flatMap {
          success =>
            success.flatMap(f) match {
              case success: IO.Success[E, O2] =>
                Some(success)

              case ex: IO.Failure[E, O2] =>
                logger.error("Failed to apply map function on Cache.", ex)
                None
            }
        }
      override def clear(): Unit = self.clear()
    }

  def mapStored[O2](f: O => IO[E, O2]): Cache[E, I, O2] =
    flatMap(Cache.concurrentIO(synchronised = false, stored = true)(f))

  def flatMap[O2](next: Cache[E, O, O2]): Cache[E, I, O2] =
    new Cache[E, I, O2] {
      //fetch the value from the lowest cache first. Upper cache should only be read if the lowest is not already computed.
      override def value(i: => I): IO[E, O2] = getOrElse(self.value(i).flatMap(next.value(_)))
      override def isCached: Boolean = self.isCached || next.isCached
      override def getOrElse(f: => IO[E, O2]): IO[E, O2] = next getOrElse f
      override def get(): Option[IO.Success[E, O2]] = next.get()
      override def clear(): Unit = {
        self.clear()
        next.clear()
      }
    }
}

private class BlockIOCache[E: ErrorHandler, I, O](cache: NoIO[I, Cache[E, I, O]]) extends Cache[E, I, O] {

  override def value(i: => I): IO[E, O] =
    cache.value(i).value(i)

  override def isCached: Boolean =
    cache.get() exists (_.isCached)

  override def getOrElse(f: => IO[E, O]): IO[E, O] =
    get() getOrElse f

  override def clear(): Unit = {
    cache.get() foreach (_.clear())
    cache.clear()
  }

  override def get(): Option[IO.Success[E, O]] =
    cache.get().flatMap(_.get())
}

private class SynchronisedIO[E: ErrorHandler, I, O](fetch: I => IO[E, O],
                                                    lazyIO: LazyIO[E, O]) extends Cache[E, I, O] {

  override def value(i: => I): IO[E, O] =
    lazyIO getOrSet fetch(i)

  override def getOrElse(f: => IO[E, O]): IO[E, O] =
    lazyIO getOrElse f

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear(): Unit =
    lazyIO.clear()

  override def get(): Option[IO.Success[E, O]] =
    lazyIO.get()
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
private class ReservedIO[E: ErrorHandler, ER <: E with Core.Error.Reserved, I, O](fetch: I => IO[E, O],
                                                                                  lazyIO: LazyIO[E, O],
                                                                                  error: ER) extends Cache[E, I, O] {

  override def value(i: => I): IO[E, O] =
    lazyIO.getOrElse {
      if (Reserve.setBusyOrGet((), error.reserve).isEmpty)
        try
          lazyIO getOrElse (lazyIO set fetch(i)) //check if it's set again in the block.
        finally
          Reserve.setFree(error.reserve)
      else
        IO.Failure(error)
    }

  override def isCached: Boolean =
    lazyIO.isDefined

  override def getOrElse(f: => IO[E, O]): IO[E, O] =
    lazyIO getOrElse f

  override def clear() =
    lazyIO.clear()

  override def get(): Option[IO.Success[E, O]] =
    lazyIO.get()
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
class NoIO[I, O](fetch: I => O, lazyValue: LazyValue[O]) {

  def value(input: => I): O =
    lazyValue getOrSet fetch(input)

  def isCached: Boolean =
    lazyValue.isDefined

  def getOrElse(f: => O): O =
    lazyValue getOrElse f

  def get() =
    lazyValue.get()

  def clear() =
    lazyValue.clear()
}
