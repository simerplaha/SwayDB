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
