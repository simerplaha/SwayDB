/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import java.util.TimerTask

import swaydb.Actor.Task
import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

final class ActorWire[I, S] private[swaydb](name: String,
                                            impl: I,
                                            interval: Option[(FiniteDuration, Long)],
                                            state: S)(implicit val ec: ExecutionContext) { wire =>

  private val actor: ActorRef[(I, S) => Unit, S] =
    interval match {
      case Some((delays, stashCapacity)) =>
        implicit val queueOrder = QueueOrder.FIFO

        Actor.timer[(I, S) => Unit, S](
          name = name,
          state = state,
          stashCapacity = stashCapacity,
          interval = delays
        ) {
          (function, self) =>
            function(impl, self.state)
        }

      case None =>
        implicit val queueOrder = QueueOrder.FIFO

        Actor[(I, S) => Unit, S](name, state) {
          (function, self) =>
            function(impl, self.state)
        }
    }

  final class Ask {
    def map[R, BAG[_]](function: (I, S) => R)(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          promise.tryComplete(Try(function(impl, state)))
      }

      bag fromPromise promise
    }

    def map[R, BAG[_]](function: (I, S, ActorWire[I, S]) => R)(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          promise.tryComplete(Try(function(impl, state, wire)))
      }

      bag fromPromise promise
    }

    def flatMap[R, BAG[_]](function: (I, S) => BAG[R])(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          bag.complete(promise, function(impl, state))
      }

      bag fromPromise promise
    }

    def flatMap[R, BAG[_]](function: (I, S, ActorWire[I, S]) => BAG[R])(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          bag.complete(promise, function(impl, state, wire))
      }

      bag fromPromise promise
    }

    def map[R, BAG[_]](delay: FiniteDuration)(function: (I, S, ActorWire[I, S]) => R)(implicit bag: Bag.Async[BAG]): Actor.Task[R, BAG] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I, state: S) => promise.tryComplete(Try(function(impl, state, wire))),
          delay = delay
        )

      new Task(bag fromPromise promise, timerTask)
    }

    def flatMap[R, BAG[_]](delay: FiniteDuration)(function: (I, S, ActorWire[I, S]) => BAG[R])(implicit bag: Bag.Async[BAG]): Actor.Task[R, BAG] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I, state: S) => bag.complete(promise, function(impl, state, wire)),
          delay = delay
        )

      new Actor.Task(bag fromPromise promise, timerTask)
    }
  }

  final val ask = new Ask

  def send[R](function: (I, S) => R): Unit =
    actor
      .send {
        (impl: I, state: S) =>
          function(impl, state)
      }

  def send[R](function: (I, S, ActorWire[I, S]) => R): Unit =
    actor
      .send {
        (impl: I, state: S) =>
          function(impl, state, this)
      }

  def send[R](delay: FiniteDuration)(function: (I, S) => R): TimerTask =
    actor.send(
      message = (impl: I, state: S) => function(impl, state),
      delay = delay
    )

  def state[BAG[_]](implicit bag: Bag.Async[BAG]): BAG[S] =
    ask
      .map {
        (_, state: S) =>
          state
      }

  def terminateAndClear[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    actor.terminateAndClear()

  def clear(): Unit =
    actor.clear()

  def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    actor.terminate()

  def isTerminated =
    actor.isTerminated
}
