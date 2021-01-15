/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import java.util.{TimerTask, UUID}
import swaydb.Actor.Task
import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object DefActor {

  @inline def apply[I, S](name: String,
                          init: DefActor[I, S] => I,
                          interval: Option[(FiniteDuration, Long)],
                          state: S)(implicit ec: ExecutionContext): DefActor[I, S] =
    new DefActor[I, S](
      name = name,
      initialiser = init,
      interval = interval,
      state = state,
      uniqueId = UUID.randomUUID()
    )
}

final class DefActor[+I, S] private(name: String,
                                    initialiser: DefActor[I, S] => I,
                                    interval: Option[(FiniteDuration, Long)],
                                    state: S,
                                    val uniqueId: UUID)(implicit val ec: ExecutionContext) { defActor =>

  private val impl = initialiser(this)

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

    def map[R, BAG[_]](function: (I, S, DefActor[I, S]) => R)(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          promise.tryComplete(Try(function(impl, state, defActor)))
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

    def flatMap[R, BAG[_]](function: (I, S, DefActor[I, S]) => BAG[R])(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I, state: S) =>
          bag.complete(promise, function(impl, state, defActor))
      }

      bag fromPromise promise
    }

    def map[R, BAG[_]](delay: FiniteDuration)(function: (I, S, DefActor[I, S]) => R)(implicit bag: Bag.Async[BAG]): Actor.Task[R, BAG] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I, state: S) => promise.tryComplete(Try(function(impl, state, defActor))),
          delay = delay
        )

      new Task(bag fromPromise promise, timerTask)
    }

    def flatMap[R, BAG[_]](delay: FiniteDuration)(function: (I, S, DefActor[I, S]) => BAG[R])(implicit bag: Bag.Async[BAG]): Actor.Task[R, BAG] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I, state: S) => bag.complete(promise, function(impl, state, defActor)),
          delay = delay
        )

      new Actor.Task(bag fromPromise promise, timerTask)
    }
  }

  final val ask = new Ask

  def send[R](function: I => R): Unit =
    actor
      .send {
        (impl: I, _: S) =>
          function(impl)
      }

  def sendWithSelf(function: I => DefActor[I, S] => Unit): Unit =
    actor
      .send {
        (impl: I, _: S) =>
          function(impl)(this)
      }

  def send[R](function: (I, S) => R): Unit =
    actor
      .send {
        (impl: I, state: S) =>
          function(impl, state)
      }

  def send[R](function: (I, S, DefActor[I, S]) => R): Unit =
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

  override def hashCode(): Int =
    uniqueId.hashCode()

  override def equals(other: Any): Boolean =
    other match {
      case other: DefActor[I, S] =>
        this.uniqueId == other.uniqueId

      case _ =>
        false
    }
}
