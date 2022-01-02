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

package swaydb.core.segment.cache.sweeper

import swaydb.ActorConfig.QueueOrder
import swaydb.{Actor, ActorConfig, ActorRef, Glass}

protected object MemorySweeperActor {

  @inline def processCommand(command: MemorySweeperCommand): Unit =
    command match {
      case command: MemorySweeperCommand.SweepKeyValue =>
        val skipListRefOrNull = command.skipListRef.underlying.get()
        val keyValueRefOrNull = command.keyValueRef.underlying.get()
        if (skipListRefOrNull != null && keyValueRefOrNull != null)
          skipListRefOrNull remove keyValueRefOrNull.key

      case block: MemorySweeperCommand.SweepBlockCache =>
        val cacheOptional = block.map.get()
        if (cacheOptional.isDefined) {
          val cache = cacheOptional.get
          cache remove block.key
          if (cache.isEmpty)
            block.map.clear()
        }

      case ref: MemorySweeperCommand.SweepSkipListMap =>
        val cacheOrNull = ref.cache.underlying.get()
        if (cacheOrNull != null)
          cacheOrNull remove ref.key

      case cache: MemorySweeperCommand.SweepCache =>
        val cacheOrNull = cache.cache.underlying.get()
        if (cacheOrNull != null)
          cacheOrNull.clear()
    }
}

protected trait MemorySweeperActor {

  def cacheSize: Long

  def actorConfig: Option[ActorConfig]

  val actor: Option[ActorRef[MemorySweeperCommand, Unit]] =
    actorConfig map {
      actorConfig =>
        Actor.cacheFromConfig[MemorySweeperCommand](
          config = actorConfig,
          stashCapacity = cacheSize,
          queueOrder = QueueOrder.FIFO,
          weigher = MemorySweeperCommand.weigher
        ) {
          (command, _) =>
            MemorySweeperActor.processCommand(command)
        }.start()
    }

  def terminateAndClear(): Unit =
    actor.foreach(_.terminateAndClear[Glass]())
}
