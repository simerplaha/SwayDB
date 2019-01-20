package swaydb.persistent

import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try
import swaydb.configs.level.{DefaultGroupingStrategy, DefaultPersistentConfig}
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
  def apply[T](dir: Path,
               maxOpenSegments: Int = 1000,
               cacheSize: Int = 100.mb,
               mapSize: Int = 4.mb,
               mmapMaps: Boolean = true,
               recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
               mmapAppendix: Boolean = true,
               mmapSegments: MMAP = MMAP.WriteAndRead,
               segmentSize: Int = 2.mb,
               appendixFlushCheckpointSize: Int = 2.mb,
               otherDirs: Seq[Dir] = Seq.empty,
               cacheCheckDelay: FiniteDuration = 7.seconds,
               segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
               bloomFilterFalsePositiveRate: Double = 0.01,
               compressDuplicateValues: Boolean = true,
               lastLevelGroupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
               acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                  keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                  ec: ExecutionContext = SwayDB.defaultExecutionContext): Try[Set[T]] = {
    CoreAPI(
      config = DefaultPersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        recoveryMode = recoveryMode,
        mapSize = mapSize,
        mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        mmapAppendix = mmapAppendix,
        segmentSize = segmentSize,
        compressDuplicateValues = compressDuplicateValues,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        groupingStrategy = lastLevelGroupingStrategy,
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

}
