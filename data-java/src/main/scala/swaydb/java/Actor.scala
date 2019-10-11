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

package swaydb.java

import java.util.concurrent.{CompletionStage, ExecutorService}
import java.util.function.BiConsumer
import java.util.{Comparator, TimerTask}

import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.java.data.TriFunctionVoid
import swaydb.java.data.util.Java.JavaFunction
import swaydb.{Scheduler, Tag}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

object Actor {

  class TerminateActor extends Throwable

  class ActorRef[T, S](val asScala: swaydb.ActorRef[T, S]) {
    implicit val tag = Tag.future(asScala.executionContext)

    def send(message: T): Unit =
      asScala.send(message)

    def ask[R](message: JavaFunction[ActorRef[R, java.lang.Void], T]): CompletionStage[R] =
      asScala.ask[R, scala.concurrent.Future] {
        actor: swaydb.ActorRef[R, Unit] =>
          message.apply(new ActorRef[R, java.lang.Void](actor.asInstanceOf[swaydb.ActorRef[R, java.lang.Void]]))
      }.toJava

    /**
     * Sends a message to this actor with delay
     */
    def send(message: T, delay: java.time.Duration, scheduler: Scheduler): TimerTask =
      asScala.send(message, delay.toScala)(scheduler)

    def ask[R](message: JavaFunction[ActorRef[R, java.lang.Void], T], delay: java.time.Duration, scheduler: Scheduler): swaydb.Actor.Task[R, CompletionStage] = {
      val javaFuture =
        asScala.ask[R, scala.concurrent.Future](
          message =
            (actor: swaydb.ActorRef[R, Unit]) =>
              message.apply(new ActorRef[R, java.lang.Void](actor.asInstanceOf[swaydb.ActorRef[R, java.lang.Void]])),
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

    def recover[M <: T](execution: TriFunctionVoid[M, Throwable, Actor[T, S]]): ActorRef[T, S] = {
      val actorRefWithRecovery =
        asScala.recover[M, Throwable] {
          case (message, io, actor) =>
            val throwable: Throwable =
              io match {
                case swaydb.IO.Right(_) =>
                  new TerminateActor()

                case swaydb.IO.Left(value) =>
                  value
              }
            execution.apply(message, throwable, new Actor(actor))
        }

      new ActorRef(actorRefWithRecovery)
    }

    def terminateAndRecover[M <: T](execution: TriFunctionVoid[M, Throwable, Actor[T, S]]): ActorRef[T, S] = {
      val actorRefWithRecovery =
        asScala.recover[M, Throwable] {
          case (message, io, actor) =>
            val throwable: Throwable =
              io match {
                case swaydb.IO.Right(_) =>
                  new TerminateActor()

                case swaydb.IO.Left(value) =>
                  value
              }
            execution.apply(message, throwable, new Actor(actor))
        }

      new ActorRef(actorRefWithRecovery)
    }
  }

  class Actor[T, S](override val asScala: swaydb.Actor[T, S]) extends ActorRef[T, S](asScala) {
    def state(): S = asScala.state
  }

  def statelessFIFO[T](execution: BiConsumer[T, Actor[T, java.lang.Void]],
                       executorService: ExecutorService): ActorRef[T, java.lang.Void] =
    statefulFIFO[T, java.lang.Void](null, execution, executorService)

  def statelessOrdered[T](execution: BiConsumer[T, Actor[T, java.lang.Void]],
                          executorService: ExecutorService,
                          comparator: Comparator[T]): ActorRef[T, java.lang.Void] =
    statefulOrdered[T, java.lang.Void](null, execution, executorService, comparator)

  def statefulFIFO[T, S](initialState: S,
                         execution: BiConsumer[T, Actor[T, S]],
                         executorService: ExecutorService): ActorRef[T, S] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, S]) =
      execution.accept(message, new Actor(actor))

    val scalaActorRef =
      swaydb.Actor[T, S](initialState)(execution = scalaExecution)(
        ec = ExecutionContext.fromExecutorService(executorService),
        queueOrder = QueueOrder.FIFO
      )

    new ActorRef(scalaActorRef)
  }

  def statefulOrdered[T, S](initialState: S,
                            execution: BiConsumer[T, Actor[T, S]],
                            executorService: ExecutorService,
                            comparator: Comparator[T]): ActorRef[T, S] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, S]) =
      execution.accept(message, new Actor(actor))

    val scalaActorRef =
      swaydb.Actor[T, S](initialState)(execution = scalaExecution)(
        ec = ExecutionContext.fromExecutorService(executorService),
        queueOrder = QueueOrder.Ordered(Ordering.comparatorToOrdering(comparator))
      )

    new ActorRef(scalaActorRef)
  }
}
