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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb

import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.data.serial.SerialThreadFactory

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Try

sealed trait Serial[T[_]] {

  def execute[F](f: => F): T[F]

  def terminate(): T[Unit]

  def terminateBag[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit]

}

object Serial {

  trait Sync[T[_]] extends Serial[T]

  trait SingleThread[T[_]] extends Serial[T] {
    def executor: ExecutorService
  }

  trait Actor[T[_]] extends Serial[T] {
    def actor: ActorRef[() => Unit, Unit]
  }

  def from[BAG[_]](implicit bag: Bag[BAG]): Serial[BAG] =
    bag match {
      case bag: Bag.Sync[BAG] =>
        sync(bag)

      case bag: Bag.Async[BAG] =>
        singleThread(bag)
    }

  def sync[BAG[_]](implicit bag: Bag.Sync[BAG]): Serial.Sync[BAG] =
    new Serial.Sync[BAG] {
      override def execute[F](f: => F): BAG[F] =
        bag.apply(f)

      override def terminate(): BAG[Unit] =
        bag.unit

      override def terminateBag[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
        bag.unit
    }

  def singleThread[BAG[_]](implicit bag: Bag.Async[BAG]): Serial.SingleThread[BAG] = {
    val ec: ExecutorService = Executors.newSingleThreadExecutor(SerialThreadFactory.create())
    singleThread(bag, ec)
  }

  def singleThread[BAG[_]](implicit bag: Bag.Async[BAG],
                           ec: ExecutorService): Serial.SingleThread[BAG] =
    new Serial.SingleThread[BAG] {

      override def executor: ExecutorService =
        ec

      override def execute[F](f: => F): BAG[F] = {
        val promise = Promise[F]

        ec.submit {
          new Callable[Unit] {
            override def call(): Unit =
              promise.success(f) //no need to watch for failure here because they are caught at the API Level.
          }
        }

        bag.fromPromise(promise)
      }

      override def terminate(): BAG[Unit] =
        bag.fromIO(IO(ec.awaitTermination(10, TimeUnit.SECONDS)))

      override def terminateBag[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
        bag.fromIO(IO(ec.awaitTermination(10, TimeUnit.SECONDS)))
    }

  def actor[BAG[_]](implicit bag: Bag.Async[BAG],
                    ec: ExecutionContext): Serial.Actor[BAG] = {
    val actor = Actor[() => Unit]("Actor Serial") {
      (run, _) =>
        run()
    }(ec, QueueOrder.FIFO)

    Serial.actor(bag, actor)
  }

  def actor[BAG[_]](implicit bag: Bag.Async[BAG],
                    actor: ActorRef[() => Unit, Unit]): Serial.Actor[BAG] =
    new Serial.Actor[BAG] {

      override def actor: ActorRef[() => Unit, Unit] =
        actor

      override def execute[F](f: => F): BAG[F] = {
        val promise = Promise[F]()
        actor.send(() => promise.success(f))
        bag.fromPromise(promise)
      }

      override def terminate(): BAG[Unit] =
        actor.terminateAndClear[BAG]()

      override def terminateBag[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
        actor.terminateAndClear[BAG]()(bag)

    }

  def transfer[BAG1[_], BAG2[_]](from: Serial[BAG1])(implicit bag1: Bag[BAG1],
                                                     bag2: Bag[BAG2]): Serial[BAG2] =
    from match {
      case _: Serial.Sync[BAG1] =>
        bag2 match {
          case bag2: Bag.Sync[BAG2] =>
            Serial.sync[BAG2](bag2)

          case bag2: Bag.Async[BAG2] =>
            Serial.singleThread[BAG2](bag2)
        }

      case from: Serial.SingleThread[BAG1] =>
        bag2 match {
          case bag2: Bag.Sync[BAG2] =>
            Serial.sync[BAG2](bag2)

          case bag2: Bag.Async[BAG2] =>
            Serial.singleThread[BAG2](bag2, from.executor)
        }

      case from: Serial.Actor[BAG1] =>
        bag2 match {
          case bag2: Bag.Sync[BAG2] =>
            Serial.sync[BAG2](bag2)

          case bag2: Bag.Async[BAG2] =>
            Serial.actor[BAG2](bag2, from.actor)
        }
    }
}
