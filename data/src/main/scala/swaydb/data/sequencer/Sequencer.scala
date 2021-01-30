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

package swaydb.data.sequencer

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag

import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}
import scala.concurrent.Promise
import scala.util.Try

sealed trait Sequencer[T[_]] {

  def execute[F](f: => F): T[F]

  def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit]

}

case object Sequencer extends LazyLogging {

  sealed trait Synchronised[BAG[_]] extends Sequencer[BAG]

  sealed trait SingleThread[BAG[_]] extends Sequencer[BAG] {
    def executor: ExecutorService
  }

  def synchronised[BAG[_]](implicit bag: Bag[BAG]): Sequencer.Synchronised[BAG] =
    new Sequencer.Synchronised[BAG] {
      override def execute[F](f: => F): BAG[F] =
        bag.apply(f)

      override def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
        bag.unit
    }

  def singleThread[BAG[_]](implicit bag: Bag.Async[BAG]): Sequencer.SingleThread[BAG] =
    singleThread(
      bag = bag,
      ec = Executors.newSingleThreadExecutor(SequencerThreadFactory.create())
    )

  def transfer[BAG1[_], BAG2[_]](from: Sequencer[BAG1])(implicit bag1: Bag[BAG1],
                                                        bag2: Bag[BAG2]): Sequencer[BAG2] =
    from match {
      case _: Sequencer.Synchronised[BAG1] =>
        bag2 match {
          case bag2: Bag.Sync[BAG2] =>
            Sequencer.synchronised[BAG2](bag2)

          case bag2: Bag.Async[BAG2] =>
            Sequencer.singleThread[BAG2](bag2)
        }

      case from: Sequencer.SingleThread[BAG1] =>
        bag2 match {
          case bag2: Bag.Sync[BAG2] =>
            Sequencer.synchronised[BAG2](bag2)

          case bag2: Bag.Async[BAG2] =>
            Sequencer.singleThread[BAG2](bag2, from.executor)
        }
    }

  private def singleThread[BAG[_]](implicit bag: Bag.Async[BAG],
                                   ec: ExecutorService): Sequencer.SingleThread[BAG] =
    new Sequencer.SingleThread[BAG] {

      override def executor: ExecutorService =
        ec

      override def execute[F](f: => F): BAG[F] = {
        val promise = Promise[F]

        ec.submit {
          new Callable[Boolean] {
            override def call(): Boolean =
              promise.tryComplete(Try(f))
          }
        }

        bag.fromPromise(promise)
      }

      override def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
        bag {
          logger.info("Terminating Serial ExecutorService.")
          ec.awaitTermination(10, TimeUnit.SECONDS)
        }
    }
}
