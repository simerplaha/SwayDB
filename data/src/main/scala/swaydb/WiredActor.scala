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

object WiredActor {
  def apply[T, S](impl: T, state: S)(implicit scheduler: Scheduler): WiredActor[T, S] =
    new WiredActor(impl, None, state)

  def apply[T, S](impl: T, interval: FiniteDuration, stashCapacity: Int, state: S)(implicit scheduler: Scheduler): WiredActor[T, S] =
    new WiredActor(impl, Some((interval, stashCapacity)), state)
}

class WiredActor[T, S](impl: T, interval: Option[(FiniteDuration, Int)], state: S)(implicit val scheduler: Scheduler) {

  implicit val ec = scheduler.ec
  implicit val queueOrder = QueueOrder.FIFO

  private val actor: Actor[() => Unit, S] = {
    interval match {
      case Some((delays, stashCapacity)) =>
        Actor.timer[() => Unit, S](
          state = state,
          stashCapacity = stashCapacity,
          interval = delays
        ) {
          (function, _) =>
            function()
        }

      case None =>
        Actor[() => Unit, S](state) {
          (function, _) =>
            function()
        }
    }
    }.asInstanceOf[Actor[() => Unit, S]]

  def unsafeGetState: S =
    actor.state

  def unsafeGetImpl: T =
    impl

  def ask[R](function: (T, S) => R): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryComplete(Try(function(impl, unsafeGetState))))
    promise.future
  }

  def askFlatMap[R](function: (T, S) => Future[R]): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryCompleteWith(function(impl, unsafeGetState)))
    promise.future
  }

  def ask[R](function: (T, S, WiredActor[T, S]) => R): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryComplete(Try(function(impl, unsafeGetState, this))))
    promise.future
  }

  def askFlatMap[R](function: (T, S, WiredActor[T, S]) => Future[R]): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryCompleteWith(function(impl, unsafeGetState, this)))
    promise.future
  }

  def send[R](function: (T, S) => R): Unit =
    actor ! (() => function(impl, unsafeGetState))

  def send[R](function: (T, S, WiredActor[T, S]) => R): Unit =
    actor ! (() => function(impl, unsafeGetState, this))

  def scheduleAsk[R](delay: FiniteDuration)(function: (T, S) => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.tryComplete(Try(function(impl, unsafeGetState))), delay)
    (promise.future, timerTask)
  }

  def scheduleAskFlatMap[R](delay: FiniteDuration)(function: (T, S) => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.completeWith(function(impl, unsafeGetState)), delay)
    (promise.future, timerTask)
  }

  def scheduleAskWithSelf[R](delay: FiniteDuration)(function: (T, S, WiredActor[T, S]) => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.tryComplete(Try(function(impl, unsafeGetState, this))), delay)
    (promise.future, timerTask)
  }

  def scheduleAskWithSelfFlatMap[R](delay: FiniteDuration)(function: (T, S, WiredActor[T, S]) => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.completeWith(function(impl, unsafeGetState, this)), delay)
    (promise.future, timerTask)
  }

  def scheduleSend[R](delay: FiniteDuration)(function: (T, S) => R): TimerTask =
    actor.schedule(() => function(impl, unsafeGetState), delay)

  def terminate(): Unit = {
    scheduler.terminate()
    actor.terminateAndClear()
  }
}