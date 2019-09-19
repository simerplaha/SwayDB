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

package swaydb

import java.util.TimerTask

import swaydb.Actor.Task
import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object ActorWire {
  def apply[I, S](impl: I, state: S)(implicit scheduler: Scheduler): ActorWire[I, S] =
    new ActorWire(impl, None, state)

  def apply[I, S](impl: I, interval: FiniteDuration, stashCapacity: Int, state: S)(implicit scheduler: Scheduler): ActorWire[I, S] =
    new ActorWire(impl, Some((interval, stashCapacity)), state)
}

class ActorWire[I, S](impl: I, interval: Option[(FiniteDuration, Int)], state: S)(implicit val scheduler: Scheduler) { self =>

  implicit def ec = scheduler.ec

  private val actor: ActorRef[(I, S) => Unit, S] =
    interval match {
      case Some((delays, stashCapacity)) =>
        implicit val queueOrder = QueueOrder.FIFO

        Actor.timer[(I, S) => Unit, S](
          state = state,
          stashCapacity = stashCapacity,
          interval = delays
        ) {
          (function, self) =>
            function(impl, self.state)
        }

      case None =>
        implicit val queueOrder = QueueOrder.FIFO

        Actor[(I, S) => Unit, S](state) {
          (function, self) =>
            function(impl, self.state)
        }
    }

  class Ask {
    def map[R, T[_]](function: (I, S) => R)(implicit tag: Tag.Async[T]): T[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          promise.tryComplete(Try(function(impl, state)))
      }

      tag fromPromise promise
    }

    def map[R, T[_]](function: (I, S, ActorWire[I, S]) => R)(implicit tag: Tag.Async[T]): T[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          promise.tryComplete(Try(function(impl, state, self)))
      }

      tag fromPromise promise
    }

    def flatMap[R, T[_]](function: (I, S) => T[R])(implicit tag: Tag.Async[T]): T[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          tag.complete(promise, function(impl, state))
      }

      tag fromPromise promise
    }

    def flatMap[R, T[_]](function: (I, S, ActorWire[I, S]) => T[R])(implicit tag: Tag.Async[T]): T[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          tag.complete(promise, function(impl, state, self))
      }

      tag fromPromise promise
    }

    def map[R, T[_]](delay: FiniteDuration)(function: (I, S, ActorWire[I, S]) => R)(implicit tag: Tag.Async[T]): Actor.Task[R, T] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I, state: S) => promise.tryComplete(Try(function(impl, state, self))),
          delay = delay
        )

      new Task(tag fromPromise promise, timerTask)
    }

    def flatMap[R, T[_]](delay: FiniteDuration)(function: (I, S, ActorWire[I, S]) => T[R])(implicit tag: Tag.Async[T]): Actor.Task[R, T] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I, state: S) => tag.complete(promise, function(impl, state, self)),
          delay = delay
        )

      new Actor.Task(tag fromPromise promise, timerTask)
    }
  }

  final val ask = new Ask

  def send[R](function: (I, S) => R): Unit =
    actor send {
      (impl: I, state: S) =>
        function(impl, state)
    }

  def send[R](function: (I, S, ActorWire[I, S]) => R): Unit =
    actor send {
      (impl: I, state: S) =>
        function(impl, state, this)
    }

  def send[R](delay: FiniteDuration)(function: (I, S) => R): TimerTask =
    actor.send(
      message = (impl: I, state: S) => function(impl, state),
      delay = delay
    )

  def state[T[_]](implicit tag: Tag.Async[T]): T[S] =
    ask
      .map(
        (_, state: S) =>
          state
      )

  def terminateAndClear(): Unit = {
    scheduler.terminate()
    actor.terminateAndClear()
  }

  def clear(): Unit =
    actor.clear()

  def terminate(): Unit =
    actor.terminate()

  def isTerminated =
    actor.isTerminated
}