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

import swaydb.Actor.Task
import swaydb.ActorConfig.QueueOrder

import java.util.{TimerTask, UUID}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Try

object DefActor {

  @inline def apply[I](name: String,
                       init: DefActor[I] => I,
                       interval: Option[(FiniteDuration, Long)])(implicit ec: ExecutionContext): DefActor.Hooks[I] =
    new Hooks[I](
      name = name,
      init = init,
      interval = interval,
      preTerminate = None,
      postTerminate = None
    )

  final class Hooks[+I](name: String,
                        init: DefActor[I] => I,
                        interval: Option[(FiniteDuration, Long)],
                        preTerminate: Option[(I, DefActor[I]) => Unit],
                        postTerminate: Option[(I, DefActor[I]) => Unit])(implicit val ec: ExecutionContext) {

    def onPreTerminate(f: (I, DefActor[I]) => Unit): Hooks[I] =
      new Hooks[I](
        name = name,
        init = init,
        interval = interval,
        preTerminate = Some(f),
        postTerminate = postTerminate
      )

    def onPostTerminate(f: (I, DefActor[I]) => Unit): Hooks[I] =
      new Hooks[I](
        name = name,
        init = init,
        interval = interval,
        preTerminate = preTerminate,
        postTerminate = Some(f)
      )

    def start(): DefActor[I] =
      new DefActor[I](
        name = name,
        initialiser = init,
        interval = interval,
        preTerminate = preTerminate,
        postTerminate = postTerminate,
        uniqueId = UUID.randomUUID()
      )
  }
}

final class DefActor[+I] private(name: String,
                                 initialiser: DefActor[I] => I,
                                 interval: Option[(FiniteDuration, Long)],
                                 preTerminate: Option[(I, DefActor[I]) => Unit],
                                 postTerminate: Option[(I, DefActor[I]) => Unit],
                                 val uniqueId: UUID)(implicit val ec: ExecutionContext) { defActor =>

  private val impl = initialiser(this)

  private val actor: ActorRef[I => Unit, Unit] = {
    val actorBoot: ActorHooks[I => Unit, Unit] =
      interval match {
        case Some((delays, stashCapacity)) =>
          implicit val queueOrder = QueueOrder.FIFO

          Actor.timer[I => Unit](
            name = name,
            stashCapacity = stashCapacity,
            interval = delays
          ) {
            (function, self) =>
              function(impl)
          }

        case None =>
          implicit val queueOrder = QueueOrder.FIFO

          Actor[I => Unit](name) {
            (function, self) =>
              function(impl)
          }
      }

    val preTerminateBoot =
      preTerminate match {
        case Some(preTerminate) =>
          actorBoot.onPreTerminate {
            _ =>
              preTerminate(impl, this)
          }

        case None =>
          actorBoot
      }

    postTerminate match {
      case Some(postTerminate) =>
        preTerminateBoot.onPostTerminate {
          _ =>
            postTerminate(impl, this)
        }.start()

      case None =>
        preTerminateBoot.start()
    }
  }

  final class Ask {
    def map[R, BAG[_]](function: I => R)(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I) =>
          promise.tryComplete(Try(function(impl)))
      }

      bag fromPromise promise
    }

    def map[R, BAG[_]](function: (I, DefActor[I]) => R)(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I) =>
          promise.tryComplete(Try(function(impl, defActor)))
      }

      bag fromPromise promise
    }

    def flatMap[R, BAG[_]](function: I => BAG[R])(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I) =>
          bag.complete(promise, function(impl))
      }

      bag fromPromise promise
    }

    def flatMap[R, BAG[_]](function: (I, DefActor[I]) => BAG[R])(implicit bag: Bag.Async[BAG]): BAG[R] = {
      val promise = Promise[R]()

      actor send {
        (impl: I) =>
          bag.complete(promise, function(impl, defActor))
      }

      bag fromPromise promise
    }

    def map[R, BAG[_]](delay: FiniteDuration)(function: (I, DefActor[I]) => R)(implicit bag: Bag.Async[BAG]): Actor.Task[R, BAG] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I) => promise.tryComplete(Try(function(impl, defActor))),
          delay = delay
        )

      new Task(bag fromPromise promise, timerTask)
    }

    def flatMap[R, BAG[_]](delay: FiniteDuration)(function: (I, DefActor[I]) => BAG[R])(implicit bag: Bag.Async[BAG]): Actor.Task[R, BAG] = {
      val promise = Promise[R]()

      val timerTask =
        actor.send(
          message = (impl: I) => bag.complete(promise, function(impl, defActor)),
          delay = delay
        )

      new Actor.Task(bag fromPromise promise, timerTask)
    }
  }

  final val ask = new Ask

  def send[R](function: I => R): Unit =
    actor
      .send {
        (impl: I) =>
          function(impl)
      }

  def sendWithSelf(function: I => DefActor[I] => Unit): Unit =
    actor
      .send {
        (impl: I) =>
          function(impl)(this)
      }

  def send[R](function: (I, DefActor[I]) => R): Unit =
    actor
      .send {
        (impl: I) =>
          function(impl, this)
      }

  def send[R](delay: FiniteDuration)(function: I => R): TimerTask =
    actor.send(
      message = (impl: I) => function(impl),
      delay = delay
    )

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
      case other: DefActor[I] =>
        this.uniqueId == other.uniqueId

      case _ =>
        false
    }
}
