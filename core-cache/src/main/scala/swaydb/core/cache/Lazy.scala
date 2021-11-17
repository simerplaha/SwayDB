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

import swaydb.IO

private[swaydb] object Lazy {

  def value[A](synchronised: Boolean,
               stored: Boolean,
               initial: Option[A]): LazyValue[A] = {
    val cache =
      new LazyValue[A](
        synchronised = synchronised,
        stored = stored
      )
    initial.foreach(value => cache.set(value))
    cache
  }

  def io[E: IO.ExceptionHandler, A](synchronised: Boolean,
                                    stored: Boolean,
                                    initial: Option[A]): LazyIO[E, A] =
    new LazyIO[E, A](
      lazyValue =
        Lazy.value[IO.Right[E, A]](
          synchronised = synchronised,
          stored = stored,
          initial = initial.map(IO.Right(_))
        )
    )
}

protected sealed trait Lazy[A] {
  def get(): Option[A]
  def set(value: => A): A
  def getOrSet(value: => A): A
  def getOrElse[B >: A](f: => B): B
  def isDefined: Boolean
  def isEmpty: Boolean
  def clear(): Unit
  def clearApply[E, T](f: Option[A] => IO[E, T]): IO[E, T]
  def stored: Boolean
  def synchronised: Boolean
}

private[swaydb] class LazyValue[A](val synchronised: Boolean, val stored: Boolean) extends Lazy[A] {

  @volatile private var cache: Option[A] = None

  override def get(): Option[A] =
    cache

  def set(value: => A): A =
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

  def getOrSet(value: => A): A =
    cache getOrElse {
      if (synchronised) {
        this.synchronized {
          cache.getOrElse {
            val got = value
            if (stored) cache = Some(got)
            got
          }
        }
      } else {
        val got = value
        if (stored) cache = Some(got)
        got
      }
    }

  override def clearApply[E, T](f: Option[A] => IO[E, T]): IO[E, T] =
    if (stored)
      if (synchronised)
        this.synchronized {
          f(get()) onRightSideEffect {
            _ =>
              clear()
          }
        }
      else
        f(get()) onRightSideEffect {
          _ =>
            clear()
        }
    else
      f(None)

  def getOrElse[B >: A](f: => B): B =
    get() getOrElse f

  def map[B](f: A => B): Option[B] =
    get() map f

  def flatMap[B >: A](f: A => Option[B]): Option[B] =
    get() flatMap f

  def isDefined: Boolean =
    get().isDefined

  def isEmpty: Boolean =
    get().isEmpty

  def clear(): Unit =
    this.cache = None
}

private[swaydb] class LazyIO[E: IO.ExceptionHandler, A](lazyValue: LazyValue[IO.Right[E, A]]) extends Lazy[IO[E, A]] {

  def stored =
    lazyValue.stored

  def synchronised: Boolean =
    lazyValue.synchronised

  def set(value: => IO[E, A]): IO[E, A] =
    try
      lazyValue set IO.Right(value.get)
    catch {
      case exception: Exception =>
        IO.failed[E, A](exception)
    }

  override def get(): Option[IO.Right[E, A]] =
    lazyValue.get()

  override def getOrSet(value: => IO[E, A]): IO[E, A] =
    try
      lazyValue getOrSet IO.Right(value.get)
    catch {
      case exception: Exception =>
        IO.failed[E, A](exception)
    }

  override def clearApply[E2, T](f: Option[IO[E, A]] => IO[E2, T]): IO[E2, T] =
    lazyValue clearApply f

  override def getOrElse[B >: IO[E, A]](f: => B): B =
    lazyValue getOrElse f

  def map[B](f: A => B): IO[E, Option[B]] =
    lazyValue
      .get()
      .map(_.map(f).toOptionValue)
      .getOrElse(IO.none)

  def flatMap[B](f: A => IO[E, B]): IO[E, Option[B]] =
    lazyValue
      .get()
      .map(_.flatMap(f).toOptionValue)
      .getOrElse(IO.none)

  override def isDefined: Boolean =
    lazyValue.isDefined

  override def isEmpty: Boolean =
    get().isEmpty

  override def clear(): Unit =
    lazyValue.clear()

}
