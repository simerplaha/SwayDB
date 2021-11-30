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
        val cacheOrNull = cache.cache.underlying.get
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
