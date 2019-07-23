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
import swaydb.IO
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.FunctionUtil
import swaydb.data.config.IOStrategy
import swaydb.data.Reserve
import swaydb.ErrorHandler.CoreError

private[core] object Cache {

  def empty[I, O](emptyOutput: O): Cache[I, O] =
    Cache.concurrentIO[I, O](synchronised = false, stored = false){
      _ =>
        IO(emptyOutput)
    }

  def emptyNoIO[I, O](empty: O): NoIO[I, O] =
    Cache.noIO(synchronised = false, stored = false) {
      _ =>
        empty
    }

  def emptyValuesBlock: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    Cache.concurrentIO(synchronised = false, stored = true) {
      _ =>
        IO(ValuesBlock.emptyUnblocked)
    }

  def concurrentIO[I, O](synchronised: Boolean, stored: Boolean)(fetch: I => IO[IO.Error, O]): Cache[I, O] =
    new SynchronisedIO[I, O](
      fetch = fetch,
      lazyIO = Lazy.io(synchronised = synchronised, stored = stored)
    )

  def reservedIO[I, O](stored: Boolean, reserveError: IO.Error.Busy)(fetch: I => IO[IO.Error, O]): Cache[I, O] =
    new ReservedIO(
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

  def blockIO[I, O](blockIO: I => IOStrategy, reserveError: => IO.Error.Busy)(fetch: I => IO[IO.Error, O]): Cache[I, O] =
    new BlockIOCache[I, O](
      Cache.noIO[I, Cache[I, O]](synchronised = false, stored = true) {
        i =>
          FunctionUtil.safe((_: I) => IOStrategy.ConcurrentIO(false), blockIO)(i) match {
            case IOStrategy.ConcurrentIO(cacheOnAccess) =>
              Cache.concurrentIO[I, O](
                synchronised = false,
                stored = cacheOnAccess
              )(fetch)

            case IOStrategy.SynchronisedIO(cacheOnAccess) =>
              Cache.concurrentIO[I, O](
                synchronised = true,
                stored = cacheOnAccess
              )(fetch)

            case IOStrategy.ReservedIO(cacheOnAccess) =>
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
private[core] sealed trait Cache[I, O] extends LazyLogging { self =>
  def value(i: => I): IO[IO.Error, O]
  def isCached: Boolean
  def clear(): Unit

  def get(): Option[IO.Success[IO.Error, O]]

  def getOrElse(f: => IO[IO.Error, O]): IO[IO.Error, O]

  def getSomeOrElse(f: => IO[IO.Error, Option[O]]): IO[IO.Error, Option[O]] =
    get().map(_.map(Some(_))) getOrElse f

  /**
    * An adapter function that applies the map function to the input on each invocation.
    * The result does not get stored in this cache.
    *
    * [[mapStored]] Or [[flatMap]] functions are used for where storage is required.
    */
  def map[O2](f: O => IO[IO.Error, O2]): Cache[I, O2] =
    new Cache[I, O2] {
      override def value(i: => I): IO[IO.Error, O2] = self.value(i).flatMap(f)
      override def isCached: Boolean = self.isCached
      override def getOrElse(f: => IO[IO.Error, O2]): IO[IO.Error, O2] = get() getOrElse f
      override def get(): Option[IO.Success[IO.Error, O2]] =
        self.get() flatMap {
          success =>
            success.flatMap(f) match {
              case success: IO.Success[IO.Error, O2] =>
                Some(success)

              case ex: IO.Failure[IO.Error, O2] =>
                logger.error("Failed to apply map function on Cache.", ex)
                None
            }
        }
      override def clear(): Unit = self.clear()
    }

  def mapStored[O2](f: O => IO[IO.Error, O2]): Cache[I, O2] =
    flatMap(Cache.concurrentIO(synchronised = false, stored = true)(f))

  def flatMap[O2](next: Cache[O, O2]): Cache[I, O2] =
    new Cache[I, O2] {
      //fetch the value from the lowest cache first. Upper cache should only be read if the lowest is not already computed.
      override def value(i: => I): IO[IO.Error, O2] = getOrElse(self.value(i).flatMap(next.value(_)))
      override def isCached: Boolean = self.isCached || next.isCached
      override def getOrElse(f: => IO[IO.Error, O2]): IO[IO.Error, O2] = next getOrElse f
      override def get(): Option[IO.Success[IO.Error, O2]] = next.get()
      override def clear(): Unit = {
        self.clear()
        next.clear()
      }
    }
}

private class BlockIOCache[I, O](cache: NoIO[I, Cache[I, O]]) extends Cache[I, O] {

  override def value(i: => I): IO[IO.Error, O] =
    cache.value(i).value(i)

  override def isCached: Boolean =
    cache.get() exists (_.isCached)

  override def getOrElse(f: => IO[IO.Error, O]): IO[IO.Error, O] =
    get() getOrElse f

  override def clear(): Unit = {
    cache.get() foreach (_.clear())
    cache.clear()
  }

  override def get(): Option[IO.Success[IO.Error, O]] =
    cache.get().flatMap(_.get())
}

private class SynchronisedIO[I, O](fetch: I => IO[IO.Error, O],
                                   lazyIO: LazyIO[O]) extends Cache[I, O] {

  override def value(i: => I): IO[IO.Error, O] =
    lazyIO getOrSet fetch(i)

  override def getOrElse(f: => IO[IO.Error, O]): IO[IO.Error, O] =
    lazyIO getOrElse f

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear(): Unit =
    lazyIO.clear()

  override def get(): Option[IO.Success[IO.Error, O]] =
    lazyIO.get()
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
private class ReservedIO[I, O](fetch: I => IO[IO.Error, O], lazyIO: LazyIO[O], error: IO.Error.Busy) extends Cache[I, O] {

  override def value(i: => I): IO[IO.Error, O] =
    lazyIO getOrElse {
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

  override def getOrElse(f: => IO[IO.Error, O]): IO[IO.Error, O] =
    lazyIO getOrElse f

  override def clear() =
    lazyIO.clear()

  override def get(): Option[IO.Success[IO.Error, O]] =
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
