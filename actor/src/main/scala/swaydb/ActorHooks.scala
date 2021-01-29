/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO.ExceptionHandler
import swaydb.data.cache.CacheNoIO

import scala.concurrent.ExecutionContext

class ActorHooks[-T, S](val name: String,
                        val state: S,
                        queue: ActorQueue[(T, Int)],
                        stashCapacity: Long,
                        weigher: T => Int,
                        cached: Boolean,
                        execution: (T, Actor[T, S]) => Unit,
                        scheduler: CacheNoIO[Unit, Scheduler],
                        interval: Option[Interval],
                        preTerminate: Option[Actor[T, S] => Unit],
                        postTerminate: Option[Actor[T, S] => Unit],
                        recovery: Option[(T, IO[Throwable, Actor.Error], Actor[T, S]) => Unit])(implicit val executionContext: ExecutionContext) extends LazyLogging {

  def recoverException[M <: T](f: (M, IO[Throwable, Actor.Error], Actor[T, S]) => Unit): ActorHooks[T, S] =
    recover[M, Throwable](f)

  def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorHooks[T, S] =
    new ActorHooks[T, S](
      name = name,
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      interval = interval,
      scheduler = scheduler,
      preTerminate = preTerminate,
      postTerminate = postTerminate,
      recovery =
        Some {
          case (message: M@unchecked, error, actor) =>
            error match {
              case IO.Right(actorError) =>
                f(message, IO.Right(actorError), actor)

              case IO.Left(throwable) =>
                f(message, IO.Left(ExceptionHandler.toError(throwable)), actor)
            }

          case (_, error, _) =>
            error match {
              case IO.Right(Actor.Error.TerminatedActor) =>
                logger.error(s"""Actor("$name") - Failed to recover failed message.""", new Exception("Cause: Terminated Actor"))

              case IO.Left(exception: Throwable) =>
                logger.error(s"""Actor("$name") - Failed to recover failed message.""", exception)
            }
        }
    )

  def onPreTerminate(f: Actor[T, S] => Unit): ActorHooks[T, S] =
    new ActorHooks[T, S](
      name = name,
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      scheduler = scheduler,
      interval = interval,
      preTerminate = Some(f),
      postTerminate = postTerminate,
      recovery = recovery
    )

  def onPostTerminate(f: Actor[T, S] => Unit): ActorHooks[T, S] =
    new ActorHooks[T, S](
      name = name,
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      scheduler = scheduler,
      interval = interval,
      preTerminate = preTerminate,
      postTerminate = Some(f),
      recovery = recovery
    )

  def start(): Actor[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      scheduler = scheduler,
      interval = interval,
      preTerminate = preTerminate,
      postTerminate = postTerminate,
      recovery = recovery
    )
}
