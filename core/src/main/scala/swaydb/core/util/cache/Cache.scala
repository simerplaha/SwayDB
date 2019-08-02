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
import swaydb.{ErrorHandler, IO}

private[core] object Cache {

  def valueIO[E: ErrorHandler, I, B](output: B): Cache[E, I, B] =
    new Cache[E, I, B] {
      override def value(i: => I): IO[E, B] =
        IO(output)

      override def isCached: Boolean =
        true

      override def clear(): Unit =
        ()

      override def get(): Option[IO.Success[E, B]] =
        Option(IO.Success(output))

      override def getOrElse[F >: E : ErrorHandler, BB >: B](f: => IO[F, BB]): IO[F, BB] =
        IO(output)
    }

  def emptyValuesBlock[E: ErrorHandler]: Cache[E, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    Cache.concurrentIO(synchronised = false, initial = None, stored = true) {
      _ =>
        IO(ValuesBlock.emptyUnblocked)
    }

  def concurrentIO[E: ErrorHandler, I, O](synchronised: Boolean,
                                          stored: Boolean,
                                          initial: Option[O])(fetch: I => IO[E, O]): Cache[E, I, O] =
    new SynchronisedIO[E, I, O](
      fetch = fetch,
      lazyIO =
        Lazy.io(
          synchronised = synchronised,
          initial = initial,
          stored = stored
        )
    )

  def reservedIO[E: ErrorHandler, ER <: E with swaydb.Error.Recoverable, I, O](stored: Boolean,
                                                                               reserveError: ER,
                                                                               initial: Option[O])(fetch: I => IO[E, O]): Cache[E, I, O] =
    new ReservedIO[E, ER, I, O](
      fetch = fetch,
      error = reserveError,
      lazyIO =
        Lazy.io(
          synchronised = false,
          initial = initial,
          stored = stored
        )
    )

  def noIO[I, O](synchronised: Boolean,
                 stored: Boolean,
                 initial: Option[O])(fetch: I => O): NoIO[I, O] =
    new NoIO[I, O](
      fetch = fetch,
      lazyValue =
        Lazy.value(
          synchronised = synchronised,
          stored = stored,
          initial = initial
        )
    )

  def io[E: ErrorHandler, ER <: E with swaydb.Error.Recoverable, I, O](strategy: IOStrategy,
                                                                       reserveError: => ER,
                                                                       initial: Option[O])(fetch: I => IO[E, O]): Cache[E, I, O] =
    strategy match {
      case IOStrategy.ConcurrentIO(cacheOnAccess) =>
        Cache.concurrentIO[E, I, O](
          synchronised = false,
          initial = initial,
          stored = cacheOnAccess
        )(fetch)

      case IOStrategy.SynchronisedIO(cacheOnAccess) =>
        Cache.concurrentIO[E, I, O](
          synchronised = true,
          initial = initial,
          stored = cacheOnAccess
        )(fetch)

      case IOStrategy.ReservedIO(cacheOnAccess) =>
        Cache.reservedIO[E, ER, I, O](
          stored = cacheOnAccess,
          initial = initial,
          reserveError = reserveError
        )(fetch)
    }

  def deferredIO[E: ErrorHandler, ER <: E with swaydb.Error.Recoverable, I, O](strategy: I => IOStrategy,
                                                                               reserveError: => ER)(fetch: I => IO[E, O]): Cache[E, I, O] =
    new BlockIOCache[E, I, O](
      Cache.noIO[I, Cache[E, I, O]](synchronised = false, stored = true, initial = None) {
        i =>
          val ioStrategy = FunctionUtil.safe((_: I) => IOStrategy.ConcurrentIO(false), strategy)(i)
          Cache.io[E, ER, I, O](
            strategy = ioStrategy,
            reserveError = reserveError,
            initial = None
          )(fetch)
      }
    )
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
private[core] sealed abstract class Cache[+E: ErrorHandler, -I, +O] extends LazyLogging { self =>
  def value(i: => I): IO[E, O]
  def isCached: Boolean
  def clear(): Unit

  def get(): Option[IO.Success[E, O]]

  def getOrElse[F >: E : ErrorHandler, B >: O](f: => IO[F, B]): IO[F, B]

  def getSomeOrElse[F >: E : ErrorHandler, B >: O](f: => IO[F, Option[B]]): IO[F, Option[B]] =
    get().map(_.map(Some(_))) getOrElse f

  /**
   * An adapter function that applies the map function to the input on each invocation.
   * The result does not get stored in this cache.
   *
   * [[mapStored]] Or [[flatMap]] functions are used for where storage is required.
   */
  def map[F >: E : ErrorHandler, B](f: O => IO[F, B]): Cache[F, I, B] =
    new Cache[F, I, B] {
      override def value(i: => I): IO[F, B] =
        self.value(i).flatMap(f)

      override def isCached: Boolean =
        self.isCached

      override def getOrElse[FF >: F : ErrorHandler, BB >: B](f: => IO[FF, BB]): IO[FF, BB] =
        get() getOrElse f

      override def get(): Option[IO.Success[F, B]] =
        self.get() flatMap {
          success =>
            success.flatMap(f) match {
              case success: IO.Success[F, B] =>
                Some(success)

              case ex: IO.Failure[F, B] =>
                logger.error("Failed to apply map function on Cache.", ex)
                None
            }
        }
      override def clear(): Unit =
        self.clear()
    }

  def mapStored[F >: E : ErrorHandler, O2](f: O => IO[F, O2]): Cache[F, I, O2] =
    flatMap(Cache.concurrentIO(synchronised = false, stored = true, initial = None)(f))

  def flatMap[F >: E : ErrorHandler, B](next: Cache[F, O, B]): Cache[F, I, B] =
    new Cache[F, I, B] {
      override def value(i: => I): IO[F, B] =
        getOrElse(self.value(i).flatMap(next.value(_)))

      override def isCached: Boolean =
        self.isCached || next.isCached

      override def getOrElse[FF >: F : ErrorHandler, BB >: B](f: => IO[FF, BB]): IO[FF, BB] =
        next getOrElse f

      override def get(): Option[IO.Success[F, B]] =
        next.get()

      override def clear(): Unit = {
        self.clear()
        next.clear()
      }
    }
}

private class BlockIOCache[E: ErrorHandler, -I, +B](cache: NoIO[I, Cache[E, I, B]]) extends Cache[E, I, B] {

  override def value(i: => I): IO[E, B] =
    cache.value(i).value(i)

  override def isCached: Boolean =
    cache.get() exists (_.isCached)

  override def getOrElse[F >: E : ErrorHandler, BB >: B](f: => IO[F, BB]): IO[F, BB] =
    get() getOrElse f

  override def clear(): Unit = {
    cache.get() foreach (_.clear())
    cache.clear()
  }

  override def get(): Option[IO.Success[E, B]] =
    cache.get().flatMap(_.get())
}

private class SynchronisedIO[E: ErrorHandler, -I, +B](fetch: I => IO[E, B],
                                                      lazyIO: LazyIO[E, B]) extends Cache[E, I, B] {

  override def value(i: => I): IO[E, B] =
    lazyIO getOrSet fetch(i)

  override def getOrElse[F >: E : ErrorHandler, BB >: B](f: => IO[F, BB]): IO[F, BB] =
    lazyIO getOrElse f

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear(): Unit =
    lazyIO.clear()

  override def get(): Option[IO.Success[E, B]] =
    lazyIO.get()
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
private class ReservedIO[E: ErrorHandler, ER <: E with swaydb.Error.Recoverable, -I, +B](fetch: I => IO[E, B],
                                                                                         lazyIO: LazyIO[E, B],
                                                                                         error: ER) extends Cache[E, I, B] {

  override def value(i: => I): IO[E, B] =
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

  override def getOrElse[F >: E : ErrorHandler, BB >: B](f: => IO[F, BB]): IO[F, BB] =
    lazyIO getOrElse f

  override def clear() =
    lazyIO.clear()

  override def get(): Option[IO.Success[E, B]] =
    lazyIO.get()
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
class NoIO[-I, +O](fetch: I => O, lazyValue: LazyValue[O]) {

  def value(input: => I): O =
    lazyValue getOrSet fetch(input)

  def isCached: Boolean =
    lazyValue.isDefined

  def getOrElse[OO >: O](f: => OO): OO =
    lazyValue getOrElse f

  def get() =
    lazyValue.get()

  def clear() =
    lazyValue.clear()
}
