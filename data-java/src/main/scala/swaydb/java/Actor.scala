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
import java.util.function.{BiConsumer, Consumer}
import java.util.{Comparator, TimerTask}

import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.java.data.TriFunctionVoid
import swaydb.data.util.Java.JavaFunction
import swaydb.{Bag, Scheduler}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

object Actor {

  final class TerminatedActor extends Throwable

  sealed trait ActorBase[T, S] {
    implicit val tag = Bag.future(asScala.executionContext)

    def asScala: swaydb.ActorRef[T, S]

    def send(message: T): Unit =
      asScala.send(message)

    def ask[R](message: JavaFunction[Actor.Ref[R, Void], T]): CompletionStage[R] =
      asScala.ask[R, scala.concurrent.Future] {
        actor: swaydb.ActorRef[R, Unit] =>
          message.apply(new Actor.Ref[R, Void](actor.asInstanceOf[swaydb.ActorRef[R, Void]]))
      }.toJava

    /**
     * Sends a message to this actor with delay
     */
    def send(message: T, delay: java.time.Duration, scheduler: Scheduler): TimerTask =
      asScala.send(message, delay.toScala)(scheduler)

    def ask[R](message: JavaFunction[Actor.Ref[R, Void], T], delay: java.time.Duration, scheduler: Scheduler): swaydb.Actor.Task[R, CompletionStage] = {
      val javaFuture =
        asScala.ask[R, scala.concurrent.Future](
          message =
            (actor: swaydb.ActorRef[R, Unit]) =>
              message.apply(new Actor.Ref[R, Void](actor.asInstanceOf[swaydb.ActorRef[R, Void]])),
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

  final class Ref[T, S](override val asScala: swaydb.ActorRef[T, S]) extends ActorBase[T, S] {
    def recover[M <: T](execution: TriFunctionVoid[M, Throwable, Instance[T, S]]): Actor.Ref[T, S] = {
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
            execution.apply(message, throwable, new Instance(actor))
        }

      new Actor.Ref(actorRefWithRecovery)
    }

    def terminateAndRecover[M <: T](execution: TriFunctionVoid[M, Throwable, Instance[T, S]]): Actor.Ref[T, S] = {
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
            execution.apply(message, throwable, new Instance(actor))
        }

      new Actor.Ref(actorRefWithRecovery)
    }
  }

  final class Instance[T, S](val asScala: swaydb.Actor[T, S]) extends ActorBase[T, S] {
    def state(): S = asScala.state
  }

  def fifo[T](consumer: Consumer[T]): Actor.Ref[T, Void] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, Void]) =
      consumer.accept(message)

    val scalaActorRef =
      swaydb.Actor[T, Void](null)(execution = scalaExecution)(
        ec = scala.concurrent.ExecutionContext.Implicits.global,
        queueOrder = QueueOrder.FIFO
      )

    new Actor.Ref(scalaActorRef)
  }

  def fifo[T](consumer: BiConsumer[T, Instance[T, Void]]): Actor.Ref[T, Void] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, Void]) =
      consumer.accept(message, new Instance(actor))

    val scalaActorRef =
      swaydb.Actor[T, Void](null)(execution = scalaExecution)(
        ec = scala.concurrent.ExecutionContext.Implicits.global,
        queueOrder = QueueOrder.FIFO
      )

    new Actor.Ref(scalaActorRef)
  }

  def fifo[T](consumer: Consumer[T],
              executorService: ExecutorService): Actor.Ref[T, Void] =
    fifo[T, Void](
      initialState = null,
      consumer =
        new BiConsumer[T, Instance[T, Void]] {
          override def accept(t: T, u: Instance[T, Void]): Unit =
            consumer.accept(t)
        },
      executorService = executorService
    )

  def fifo[T](consumer: BiConsumer[T, Instance[T, Void]],
              executorService: ExecutorService): Actor.Ref[T, Void] =
    fifo[T, Void](null, consumer, executorService)

  def ordered[T](consumer: Consumer[T],
                 comparator: Comparator[T]): Actor.Ref[T, Void] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, Void]) =
      consumer.accept(message)

    val scalaActorRef =
      swaydb.Actor[T, Void](null)(execution = scalaExecution)(
        ec = scala.concurrent.ExecutionContext.Implicits.global,
        queueOrder = QueueOrder.Ordered(Ordering.comparatorToOrdering(comparator))
      )

    new Actor.Ref(scalaActorRef)
  }

  def ordered[T](consumer: BiConsumer[T, Instance[T, Void]],
                 comparator: Comparator[T]): Actor.Ref[T, Void] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, Void]) =
      consumer.accept(message, new Instance(actor))

    val scalaActorRef =
      swaydb.Actor[T, Void](null)(execution = scalaExecution)(
        ec = scala.concurrent.ExecutionContext.Implicits.global,
        queueOrder = QueueOrder.Ordered(Ordering.comparatorToOrdering(comparator))
      )

    new Actor.Ref(scalaActorRef)
  }

  def ordered[T](consumer: Consumer[T],
                 executorService: ExecutorService,
                 comparator: Comparator[T]): Actor.Ref[T, Void] =
    ordered[T, Void](
      initialState = null,
      consumer =
        new BiConsumer[T, Instance[T, Void]] {
          override def accept(t: T, u: Instance[T, Void]): Unit =
            consumer.accept(t)
        },
      executorService = executorService,
      comparator = comparator
    )

  def ordered[T](consumer: BiConsumer[T, Instance[T, Void]],
                 executorService: ExecutorService,
                 comparator: Comparator[T]): Actor.Ref[T, Void] =
    ordered[T, Void](null, consumer, executorService, comparator)

  def fifo[T, S](initialState: S,
                 consumer: Consumer[T]): Actor.Ref[T, S] =
    fifo[T, S](
      initialState = initialState,
      consumer =
        new BiConsumer[T, Instance[T, S]] {
          override def accept(t: T, u: Instance[T, S]): Unit =
            consumer.accept(t)
        }
    )

  def fifo[T, S](initialState: S,
                 consumer: BiConsumer[T, Instance[T, S]]): Actor.Ref[T, S] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, S]) =
      consumer.accept(message, new Instance(actor))

    val scalaActorRef =
      swaydb.Actor[T, S](initialState)(execution = scalaExecution)(
        ec = scala.concurrent.ExecutionContext.Implicits.global,
        queueOrder = QueueOrder.FIFO
      )

    new Actor.Ref(scalaActorRef)
  }

  def fifo[T, S](initialState: S,
                 consumer: Consumer[T],
                 executorService: ExecutorService): Actor.Ref[T, S] =
    fifo[T, S](
      initialState = initialState,
      consumer =
        new BiConsumer[T, Instance[T, S]] {
          override def accept(t: T, u: Instance[T, S]): Unit =
            consumer.accept(t)
        },
      executorService = executorService
    )

  def fifo[T, S](initialState: S,
                 consumer: BiConsumer[T, Instance[T, S]],
                 executorService: ExecutorService): Actor.Ref[T, S] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, S]) =
      consumer.accept(message, new Instance(actor))

    val scalaActorRef =
      swaydb.Actor[T, S](initialState)(execution = scalaExecution)(
        ec = ExecutionContext.fromExecutorService(executorService),
        queueOrder = QueueOrder.FIFO
      )

    new Actor.Ref(scalaActorRef)
  }

  def ordered[T, S](initialState: S,
                    consumer: Consumer[T],
                    executorService: ExecutorService,
                    comparator: Comparator[T]): Actor.Ref[T, S] = {
    ordered[T, S](
      initialState = initialState,
      consumer =
        new BiConsumer[T, Instance[T, S]] {
          override def accept(t: T, u: Instance[T, S]): Unit =
            consumer.accept(t)
        },
      executorService = executorService,
      comparator = comparator
    )
  }

  def ordered[T, S](initialState: S,
                    consumer: BiConsumer[T, Instance[T, S]],
                    executorService: ExecutorService,
                    comparator: Comparator[T]): Actor.Ref[T, S] = {
    def scalaExecution(message: T, actor: swaydb.Actor[T, S]) =
      consumer.accept(message, new Instance(actor))

    val scalaActorRef =
      swaydb.Actor[T, S](initialState)(execution = scalaExecution)(
        ec = ExecutionContext.fromExecutorService(executorService),
        queueOrder = QueueOrder.Ordered(Ordering.comparatorToOrdering(comparator))
      )

    new Actor.Ref(scalaActorRef)
  }
}
