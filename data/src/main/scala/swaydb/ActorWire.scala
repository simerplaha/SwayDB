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

import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.Try

object ActorWire {
  def apply[I, S](impl: I, state: S)(implicit scheduler: Scheduler): ActorWire[I, S] =
    new ActorWire(impl, None, state)

  def apply[I, S](impl: I, interval: FiniteDuration, stashCapacity: Int, state: S)(implicit scheduler: Scheduler): ActorWire[I, S] =
    new ActorWire(impl, Some((interval, stashCapacity)), state)
}

class ActorWire[I, S](impl: I, interval: Option[(FiniteDuration, Int)], state: S)(implicit val scheduler: Scheduler) {

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

  def ask[R](function: (I, S) => R): Future[R] = {
    val promise = Promise[R]()

    actor ! {
      (impl: I, state: S) =>
        promise.tryComplete(Try(function(impl, state)))
    }

    promise.future
  }

  def askFlatMap[R](function: (I, S) => Future[R]): Future[R] = {
    val promise = Promise[R]()

    actor ! {
      (impl: I, state: S) =>
        promise.tryCompleteWith(function(impl, state))
    }

    promise.future
  }

  def ask[R](function: (I, S, ActorWire[I, S]) => R): Future[R] = {
    val promise = Promise[R]()

    actor ! {
      (impl: I, state: S) =>
        promise.tryComplete(Try(function(impl, state, this)))
    }

    promise.future
  }

  def askFlatMap[R](function: (I, S, ActorWire[I, S]) => Future[R]): Future[R] = {
    val promise = Promise[R]()

    actor ! {
      (impl: I, state: S) =>
        promise.tryCompleteWith(function(impl, state, this))
    }

    promise.future
  }

  def send[R](function: (I, S) => R): Unit =
    actor ! {
      (impl: I, state: S) =>
        function(impl, state)
    }

  def send[R](function: (I, S, ActorWire[I, S]) => R): Unit =
    actor ! {
      (impl: I, state: S) =>
        function(impl, state, this)
    }

  def scheduleAsk[R](delay: FiniteDuration)(function: (I, S) => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()

    val timerTask =
      actor.schedule(
        message = (impl: I, state: S) => promise.tryComplete(Try(function(impl, state))),
        delay = delay
      )

    (promise.future, timerTask)
  }

  def scheduleAskFlatMap[R](delay: FiniteDuration)(function: (I, S) => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()

    val timerTask =
      actor.schedule(
        message = (impl: I, state: S) => promise.completeWith(function(impl, state)),
        delay = delay
      )

    (promise.future, timerTask)
  }

  def scheduleAskWithSelf[R](delay: FiniteDuration)(function: (I, S, ActorWire[I, S]) => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()

    val timerTask =
      actor.schedule(
        message = (impl: I, state: S) => promise.tryComplete(Try(function(impl, state, this))),
        delay = delay
      )

    (promise.future, timerTask)
  }

  def scheduleAskWithSelfFlatMap[R](delay: FiniteDuration)(function: (I, S, ActorWire[I, S]) => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()

    val timerTask =
      actor.schedule(
        message = (impl: I, state: S) => promise.completeWith(function(impl, state, this)),
        delay = delay
      )

    (promise.future, timerTask)
  }

  def state: Future[S] =
    ask(
      (_, state: S) =>
        state
    )

  def scheduleSend[R](delay: FiniteDuration)(function: (I, S) => R): TimerTask =
    actor.schedule(
      message = (impl: I, state: S) => function(impl, state),
      delay = delay
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