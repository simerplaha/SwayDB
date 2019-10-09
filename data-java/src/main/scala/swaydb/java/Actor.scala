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

import java.util.Comparator
import java.util.concurrent.ExecutorService
import java.util.function.BiConsumer

import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, ActorRef}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext

object Actor {

  def statelessFIFO[T](execution: BiConsumer[T, Actor[T, java.lang.Void]],
                       executorService: ExecutorService): ActorRef[T, java.lang.Void] =
    swaydb.Actor[T, java.lang.Void](null)(execution = execution.asScala)(
      ec = ExecutionContext.fromExecutorService(executorService),
      queueOrder = QueueOrder.FIFO
    )

  def statelessOrdered[T](execution: BiConsumer[T, Actor[T, java.lang.Void]],
                          executorService: ExecutorService,
                          comparator: Comparator[T]): ActorRef[T, java.lang.Void] =
    swaydb.Actor[T, java.lang.Void](null)(execution = execution.asScala)(
      ec = ExecutionContext.fromExecutorService(executorService),
      queueOrder = QueueOrder.Ordered(Ordering.comparatorToOrdering(comparator))
    )
}
