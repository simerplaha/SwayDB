package swaydb.eventual.persistent

import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try
import swaydb.configs.level.{DefaultGroupingStrategy, DefaultMemoryPersistentConfig}
import swaydb.core.CoreAPI
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Set, SwayDB}

object Set extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    */
  def memoryPersistentSet[T](dir: Path,
                             maxOpenSegments: Int = 1000,
                             mapSize: Int = 4.mb,
                             maxMemoryLevelSize: Int = 100.mb,
                             maxSegmentsToPush: Int = 5,
                             memoryLevelSegmentSize: Int = 2.mb,
                             persistentLevelSegmentSize: Int = 4.mb,
                             persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                             mmapPersistentSegments: MMAP = MMAP.WriteAndRead,
                             mmapPersistentAppendix: Boolean = true,
                             cacheSize: Int = 100.mb,
                             otherDirs: Seq[Dir] = Seq.empty,
                             cacheCheckDelay: FiniteDuration = 7.seconds,
                             segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                             bloomFilterFalsePositiveRate: Double = 0.01,
                             compressDuplicateValues: Boolean = true,
                             groupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
                             acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                                keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                ec: ExecutionContext = SwayDB.defaultExecutionContext): Try[Set[T]] =
    CoreAPI(
      config =
        DefaultMemoryPersistentConfig(
          dir = dir,
          otherDirs = otherDirs,
          mapSize = mapSize,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          persistentLevelSegmentSize = persistentLevelSegmentSize,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          mmapPersistentSegments = mmapPersistentSegments,
          mmapPersistentAppendix = mmapPersistentAppendix,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues,
          groupingStrategy = groupingStrategy,
          acceleration = acceleration
        ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        swaydb.Set[T](new SwayDB(core))
    }

}
