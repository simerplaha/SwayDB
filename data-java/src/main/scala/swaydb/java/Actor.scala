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
 */

package swaydb.java

import java.util.concurrent.{CompletionStage, ExecutorService}
import java.util.function.BiConsumer
import java.util.{Comparator, TimerTask}

import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.java.data.TriFunctionVoid
import swaydb.data.util.Java.JavaFunction
import swaydb.{Scheduler, Bag}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

object Actor {

  final class TerminatedActor extends Throwable

  trait ActorBase[T, S] {
    implicit val tag = Bag.future(asScala.executionContext)

    def asScala: swaydb.ActorRef[T, S]

    def send(message: T): Unit =
      asScala.send(message)

    def ask[R](message: JavaFunction[ActorRef[R, Void], T]): CompletionStage[R] =
      asScala.ask[R, scala.concurrent.Future] {
        actor: swaydb.ActorRef[R, Unit] =>
          message.apply(new ActorRef[R, Void](actor.asInstanceOf[swaydb.ActorRef[R, Void]]))
      }.toJava

    /**
     * Sends a message to this actor with delay
     */
    def send(message: T, delay: java.time.Duration, scheduler: Scheduler): TimerTask =
      asScala.send(message, delay.toScala)(scheduler)

    def ask[R](message: JavaFunction[ActorRef[R, Void], T], delay: java.time.Duration, scheduler: Scheduler): swaydb.Actor.Task[R, CompletionStage] = {
      val javaFuture =
        asScala.ask[R, scala.concurrent.Future](
          message =
            (actor: swaydb.ActorRef[R, Unit]) =>
              message.apply(new ActorRef[R, Void](actor.asInstanceOf[swaydb.ActorRef[R, Void]])),
          delay =
            delay.toScala
        )(scheduler, tag)

      new swaydb.Actor.Task(javaFuture.task.toJava, javaFuture.timer)
    }

    def totalWeight: Int =
      asScala.totalWeight

    def messageCount: Int =
      asScala.messageCount

    def hasMessages: Boolean =
      asScala.hasMessages

    def terminate(): Unit =
      asScala.terminate()

    def isTerminated: Boolean =
      asScala.isTerminated

    def clear(): Unit =
      asScala.clear()

    def terminateAndClear(): Unit =
      asScala.terminateAndClear()
  }

  final class ActorRef[T, S](override val asScala: swaydb.ActorRef[T, S]) extends ActorBase[T, S] {
    def recover[M <: T](execution: TriFunctionVoid[M, Throwable, ActorInstance[T, S]]): ActorRef[T, S] = {
      val actorRefWithRecovery =
        asScala.recover[M, Throwable] {
          case (message, io, actor) =>
            val throwable: Throwable =
              io match {
                case swaydb.IO.Right(_) =>
                  new TerminatedActor()

                case swaydb.IO.Left(value) =>
                  value
              }
            execution.apply(message, throwable, new ActorInstance(actor))
        }

      new ActorRef(actorRefWithRecovery)
    }

    def terminateAndRecover[M <: T](execution: TriFunctionVoid[M, Throwable, ActorInstance[T, S]]): ActorRef[T, S] = {
      val actorRefWithRecovery =
        asScala.recover[M, Throwable] {
          case (message, io, actor) =>
            val throwable: Throwable =
              io match {
                case swaydb.IO.Right(_) =>
                  new TerminatedActor()

                case swaydb.IO.Left(value) =>
                  value
              }
            execution.apply(message, throwable, new ActorInstance(actor))
        }

      new ActorRef(actorRefWithRecovery)
    }
  }

  final class ActorInstance[T, S](val asScala: swaydb.Actor[T, S]) extends ActorBase[T, S] {
    def state(): S = asScala.state
  }

  def createStatelessFIFO[T](execution: BiConsumer[T, ActorInstance[T, Void]],
                             executorService: ExecutorService): ActorRef[T, Void] =
    createStatefulFIFO[T, Void](null, execution, executorService)

  def createStatelessOrdered[T](execution: BiConsumer[T, ActorInstance[T, Void]],
                                executorService: ExecutorService,
                                comparator: Comparator[T]): ActorRef[T, Void] =
    createStatefulOrdered[T, Void](null, execution, executorService, comparator)

  def createStatefulFIFO[T, S](initialState: S,
                               execution: BiConsumer[T, ActorInstance[T, S]],
                               executorService: ExecutorService): ActorRef[T, S] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, S]) =
      execution.accept(message, new ActorInstance(actor))

    val scalaActorRef =
      swaydb.Actor[T, S](initialState)(execution = scalaExecution)(
        ec = ExecutionContext.fromExecutorService(executorService),
        queueOrder = QueueOrder.FIFO
      )

    new ActorRef(scalaActorRef)
  }

  def createStatefulOrdered[T, S](initialState: S,
                                  execution: BiConsumer[T, ActorInstance[T, S]],
                                  executorService: ExecutorService,
                                  comparator: Comparator[T]): ActorRef[T, S] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, S]) =
      execution.accept(message, new ActorInstance(actor))

    val scalaActorRef =
      swaydb.Actor[T, S](initialState)(execution = scalaExecution)(
        ec = ExecutionContext.fromExecutorService(executorService),
        queueOrder = QueueOrder.Ordered(Ordering.comparatorToOrdering(comparator))
      )

    new ActorRef(scalaActorRef)
  }
}
