/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.cache

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.effect.{IOStrategy, Reserve}
import swaydb.utils.{FunctionSafe, Options}

sealed trait CacheOrNull[+E, -I, +O]

private[swaydb] object Cache {

  final case class Null[-I]() extends CacheOrNull[Nothing, I, Nothing]

  def valueIO[E: IO.ExceptionHandler, I, B](output: B): Cache[E, I, B] =
    new Cache[E, I, B] {
      override def getOrFetch(input: => I): IO[E, B] =
        IO(output)

      override def isCached: Boolean =
        true

      override def isStored: Boolean =
        true

      override def clear(): Unit =
        ()

      override def state(): Option[IO.Right[E, B]] =
        Option(IO.Right(output))

      override def getOrElse[F >: E : IO.ExceptionHandler, BB >: B](f: => IO[F, BB]): IO[F, BB] =
        IO[F, BB](output)

      override def clearApply[F >: E : IO.ExceptionHandler, T](f: Option[B] => IO[F, T]): IO[F, T] =
        IO.failed[F, T](new Exception("ValueIO cannot be cleared"))
    }

  //  def emptyValuesBlock[E: IO.ExceptionHandler]: Cache[E, ValuesBlockOffset, UnblockedReader[ValuesBlockOffset, ValuesBlock]] =
  //    Cache.concurrentIO[E, ValuesBlockOffset, UnblockedReader[ValuesBlockOffset, ValuesBlock]](synchronised = false, initial = None, stored = true) {
  //      case (_, _) =>
  //        IO(ValuesBlock.emptyUnblocked)
  //    }

  def concurrentIO[E: IO.ExceptionHandler, I, O](synchronised: Boolean,
                                                 stored: Boolean,
                                                 initial: Option[O])(fetch: (I, Cache[E, I, O]) => IO[E, O]): Cache[E, I, O] =
    new SynchronisedIO[E, I, O](
      fetch = fetch,
      lazyIO =
        Lazy.io(
          synchronised = synchronised,
          initial = initial,
          stored = stored
        )
    )

  def reservedIO[E: IO.ExceptionHandler, ER <: E with swaydb.Error.Recoverable, I, O](stored: Boolean,
                                                                                      reserveError: ER,
                                                                                      initial: Option[O])(fetch: (I, Cache[E, I, O]) => IO[E, O]): Cache[E, I, O] =
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
                 initial: Option[O])(fetch: (I, CacheNoIO[I, O]) => O): CacheNoIO[I, O] =
    new CacheNoIO[I, O](
      fetch = fetch,
      lazyValue =
        Lazy.value(
          synchronised = synchronised,
          stored = stored,
          initial = initial
        )
    )

  def io[E: IO.ExceptionHandler, ER <: E with swaydb.Error.Recoverable, I, O](strategy: IOStrategy,
                                                                              reserveError: => ER,
                                                                              initial: Option[O])(fetch: (I, Cache[E, I, O]) => IO[E, O]): Cache[E, I, O] =
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

      case IOStrategy.AsyncIO(cacheOnAccess) =>
        Cache.reservedIO[E, ER, I, O](
          stored = cacheOnAccess,
          initial = initial,
          reserveError = reserveError
        )(fetch)
    }

  def deferredIO[E: IO.ExceptionHandler, ER <: E with swaydb.Error.Recoverable, I, O](initial: Option[O],
                                                                                      strategy: I => IOStrategy,
                                                                                      reserveError: => ER)(onInitialSet: (O, Cache[E, I, O]) => Unit = (_: O, _: Cache[E, I, O]) => ())(fetch: (I, Cache[E, I, O]) => IO[E, O]): Cache[E, I, O] = {

    def innerCache(ioStrategy: IOStrategy, initial: Option[O]): Cache[E, I, O] =
      Cache.io[E, ER, I, O](
        strategy = ioStrategy,
        reserveError = reserveError,
        initial = initial
      )(fetch)

    val initialInner: Option[Cache[E, I, O]] =
      if (initial.isDefined) {
        val cache = innerCache(IOStrategy.ConcurrentIO(true), initial)
        onInitialSet(initial.get, cache)
        Some(cache)
      } else {
        None
      }

    val cache =
      Cache.noIO[I, Cache[E, I, O]](synchronised = true, stored = true, initial = initialInner) {
        (i, _) =>
          val ioStrategy: IOStrategy = FunctionSafe.safe((_: I) => IOStrategy.SynchronisedIO(false), strategy)(i)
          innerCache(ioStrategy, None)
      }

    new DeferredIO[E, I, O](cache)
  }
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
private[swaydb] sealed abstract class Cache[+E: IO.ExceptionHandler, -I, +O] extends CacheOrNull[E, I, O] with LazyLogging { self =>
  def getOrFetch(input: => I): IO[E, O]
  def isCached: Boolean
  def isStored: Boolean
  def clear(): Unit
  def state(): Option[IO.Right[E, O]]
  def get(): Option[O] = state().map(_.get)

  /**
   * Atomically applies the function to the stored value
   * before clearing.
   *
   * This currently is only implemented for [[IOStrategy.ThreadSafe]] io strategies
   * and is being used when closing [[swaydb.core.file.CoreFile]]s.
   */
  def clearApply[F >: E : IO.ExceptionHandler, T](f: Option[O] => IO[F, T]): IO[F, T]

  def getOrElse[F >: E : IO.ExceptionHandler, B >: O](f: => IO[F, B]): IO[F, B]

  def getSomeOrElse[F >: E : IO.ExceptionHandler, B >: O](f: => IO[F, Option[B]]): IO[F, Option[B]] =
    state().map(_.toOptionValue) getOrElse f

  /**
   * An adapter function that applies the map function to the input on each invocation.
   * The result does not get stored in this cache.
   *
   * [[mapConcurrentStored]] Or [[flatMap]] functions are used for where storage is required.
   */
  def map[F >: E : IO.ExceptionHandler, B](f: O => IO[F, B]): Cache[F, I, B] =
    new Cache[F, I, B] {
      override def getOrFetch(input: => I): IO[F, B] =
        self.getOrFetch(input).flatMap(f)

      override def isCached: Boolean =
        self.isCached

      override def isStored: Boolean =
        self.isStored

      override def getOrElse[FF >: F : IO.ExceptionHandler, BB >: B](f: => IO[FF, BB]): IO[FF, BB] =
        state() getOrElse f

      override def state(): Option[IO.Right[F, B]] =
        self.state() flatMap {
          success =>
            success.flatMap(f) match {
              case success: IO.Right[F, B] =>
                Some(success)

              case ex: IO.Left[F, B] =>
                logger.error("Failed to apply map function on Cache.", ex.exception)
                None
            }
        }

      override def clear(): Unit =
        self.clear()

      override def clearApply[F2 >: F : IO.ExceptionHandler, T](function: Option[B] => IO[F2, T]): IO[F2, T] =
        ??? //TODO - currently not used.
    }

  //  def mapConcurrentStored[F >: E : IO.ExceptionHandler, O2](f: (O, Cache[F, I, O2]) => IO[F, O2]): Cache[F, I, O2] =
  //    flatMap(Cache.concurrentIO[F, I, O2](synchronised = false, stored = true, initial = None)(f))

  def flatMap[F >: E : IO.ExceptionHandler, B](next: Cache[F, O, B]): Cache[F, I, B] =
    new Cache[F, I, B] {
      override def getOrFetch(input: => I): IO[F, B] =
        getOrElse(self.getOrFetch(input).flatMap(next.getOrFetch(_)))

      override def isCached: Boolean =
        self.isCached || next.isCached

      override def isStored: Boolean =
        self.isStored || next.isStored

      override def getOrElse[FF >: F : IO.ExceptionHandler, BB >: B](f: => IO[FF, BB]): IO[FF, BB] =
        next getOrElse f

      /**
       * If [[next]] is not already cached see if [[self]] is cached
       * and send it's value to [[next]]'s cache to populate.
       */
      override def state(): Option[IO.Right[F, B]] =
        next.state() orElse {
          self.state() flatMap {
            value =>
              next.getOrFetch(value.get) match {
                case success @ IO.Right(_) =>
                  Some(success)

                case failure @ IO.Left(_) =>
                  logger.error("Failed to apply flatMap function on Cache.", failure.exception)
                  None
              }
          }
        }

      override def clear(): Unit = {
        next.clear()
        self.clear()
      }

      override def clearApply[F2 >: F : IO.ExceptionHandler, T](f: Option[B] => IO[F2, T]): IO[F2, T] =
        ??? //TODO - currently not used.
    }
}

private class DeferredIO[E: IO.ExceptionHandler, -I, +B](cache: CacheNoIO[I, Cache[E, I, B]]) extends Cache[E, I, B] {

  override def isStored: Boolean =
    cache.isStored

  override def getOrFetch(input: => I): IO[E, B] = {
    //ensure that i is not executed multiple times.
    var executed: I = null.asInstanceOf[I]

    def fetch =
      if (executed == null) {
        executed = input
        executed
      } else {
        executed
      }

    cache.getOrFetch(fetch).getOrFetch(fetch)
  }

  override def isCached: Boolean =
    cache.get() exists (_.isCached)

  override def getOrElse[F >: E : IO.ExceptionHandler, BB >: B](f: => IO[F, BB]): IO[F, BB] =
    state() getOrElse f

  override def clear(): Unit = {
    cache.get() foreach (_.clear())
    cache.clear()
  }

  override def state(): Option[IO.Right[E, B]] =
    cache.get().flatMap(_.state())

  override def clearApply[F >: E : IO.ExceptionHandler, T](f: Option[B] => IO[F, T]): IO[F, T] =
    ??? //TODO - currently not used.
}

private class SynchronisedIO[E: IO.ExceptionHandler, -I, +B](fetch: (I, Cache[E, I, B]) => IO[E, B],
                                                             lazyIO: LazyIO[E, B]) extends Cache[E, I, B] {

  def isStored: Boolean =
    lazyIO.stored

  override def getOrFetch(input: => I): IO[E, B] =
    lazyIO getOrSet fetch(input, this)

  override def getOrElse[F >: E : IO.ExceptionHandler, BB >: B](f: => IO[F, BB]): IO[F, BB] =
    lazyIO getOrElse f

  override def isCached: Boolean =
    lazyIO.isDefined

  override def clear(): Unit =
    lazyIO.clear()

  override def state(): Option[IO.Right[E, B]] =
    lazyIO.get()

  override def clearApply[F >: E : IO.ExceptionHandler, T](f: Option[B] => IO[F, T]): IO[F, T] =
    if (!lazyIO.synchronised) //todo should use types to disable non threadsafe IOStrategies.
      IO.Left[F, T](throw new Exception("Implemented for only IOStrategies.ThreadSafe."))
    else
      lazyIO.clearApply {
        case Some(value) =>
          value.flatMap(value => f(Some(value)))

        case None =>
          f(None)
      }
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
private class ReservedIO[E: IO.ExceptionHandler, ER <: E with swaydb.Error.Recoverable, -I, +O](fetch: (I, Cache[E, I, O]) => IO[E, O],
                                                                                                lazyIO: LazyIO[E, O],
                                                                                                error: ER) extends Cache[E, I, O] {

  def isStored: Boolean =
    lazyIO.stored

  @inline private def reserveAndExecute[F >: E : IO.ExceptionHandler, T](thunk: => IO[F, T]): IO[F, T] =
    if (Reserve.compareAndSet(Options.unit, error.reserve))
      try
        thunk
      finally
        Reserve.setFree(error.reserve)
    else
      IO.Left[F, T](error)

  override def getOrFetch(input: => I): IO[E, O] =
    lazyIO getOrElse reserveAndExecute {
      lazyIO getOrElse (lazyIO set fetch(input, this)) //check if it's set again in the block.
    }

  override def isCached: Boolean =
    lazyIO.isDefined

  override def getOrElse[F >: E : IO.ExceptionHandler, BB >: O](f: => IO[F, BB]): IO[F, BB] =
    lazyIO getOrElse f

  override def clear() =
    lazyIO.clear()

  override def state(): Option[IO.Right[E, O]] =
    lazyIO.get()

  override def clearApply[F >: E : IO.ExceptionHandler, T](f: Option[O] => IO[F, T]): IO[F, T] =
    reserveAndExecute {
      lazyIO.clearApply {
        case Some(value: IO[E, O]) =>
          value.flatMap(result => f(Some(result)))

        case None =>
          f(None)
      } //check if it's set again in the block.
    }
}

/**
 * Caches a value on read. Used for IO operations where the output does not change.
 * For example: A file's size.
 */
private[swaydb] class CacheNoIO[-I, +O](fetch: (I, CacheNoIO[I, O]) => O, lazyValue: LazyValue[O]) {

  def isStored: Boolean =
    lazyValue.stored

  def getOrFetch(input: => I): O =
    lazyValue getOrSet fetch(input, this)

  def applyOrFetchApply[E: IO.ExceptionHandler, T](apply: O => IO[E, T], fetch: => IO[E, I]): IO[E, T] =
    lazyValue.get() match {
      case Some(input) =>
        apply(input)

      case None =>
        fetch flatMap {
          input =>
            apply(getOrFetch(input))
        }
    }

  def isCached: Boolean =
    lazyValue.isDefined

  def getOrElse[OO >: O](f: => OO): OO =
    lazyValue getOrElse f

  def get(): Option[O] =
    lazyValue.get()

  def clear(): Unit =
    lazyValue.clear()
}
