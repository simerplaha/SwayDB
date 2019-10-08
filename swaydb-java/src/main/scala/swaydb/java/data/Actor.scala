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

package swaydb.java.data

import java.util.TimerTask
import java.util.concurrent.{ExecutorService, Future}
import java.util.function.BiConsumer

import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.ExecutionContext

object Actor {

  class Asked[R](val future: Future[R], val task: TimerTask)

  def statelessFIFO[T](execution: BiConsumer[T, StatelessActor[T]],
                       executorService: ExecutorService): StatelessActor[T] = {

    val scalaExecution =
      (message: T, actor: swaydb.Actor[T, Unit]) => {
        execution.accept(message, new StatelessActor(actor))
      }

    val executionContext = ExecutionContext.fromExecutorService(executorService)
    val scalaActor = swaydb.Actor[T](scalaExecution)(executionContext, QueueOrder.FIFO)
    new StatelessActor[T](scalaActor)
  }
}

