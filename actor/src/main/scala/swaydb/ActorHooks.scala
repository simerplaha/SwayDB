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

package swaydb

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO.ExceptionHandler
import swaydb.core.cache.CacheNoIO

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
