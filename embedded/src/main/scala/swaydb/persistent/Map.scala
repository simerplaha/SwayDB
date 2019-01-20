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
import swaydb.{Map, SwayDB}

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * A pre-configured, 8 Leveled, persistent database where Level1 accumulates a minimum of 10 Segments before
    * pushing Segments to lower Level.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    *
    * @param dir                         Root directory for all Level where appendix folder & files are created
    * @param otherDirs                   Secondary directories for all Levels where Segments get distributed.
    * @param maxOpenSegments             Number of concurrent Segments opened
    * @param cacheSize                   Size of in-memory key-values
    * @param mapSize                     Size of LevelZero's maps (WAL)
    * @param mmapMaps                    Memory-maps LevelZero maps files if set to true else reverts java.nio.FileChannel
    * @param mmapAppendix                Memory-maps Levels appendix files if set to true else reverts java.nio.FileChannel
    * @param mmapSegments                Memory-maps Levels Segment files if set to true else reverts java.nio.FileChannel
    * @param segmentSize                 Minimum size of Segment files in each Level
    * @param appendixFlushCheckpointSize Size of the appendix file before it's flushed. Appendix files are append only log files.
    *                                    Flushing removes deleted entries in the file hence reducing the size of the file.
    * @param cacheCheckDelay             Sets the max interval at which key-values get dropped from the cache. The delays
    *                                    are dynamically adjusted based on the current size of the cache to stay close the set
    *                                    cacheSize.
    * @param segmentsOpenCheckDelay      Sets the max interval at which Segments get closed. The delays
    *                                    are dynamically adjusted based on the current number of open Segments.
    * @param acceleration                Controls the write speed.
    * @param keySerializer               Converts keys to Bytes
    * @param valueSerializer             Converts values to Bytes
    * @param ordering                    Sort order for keys
    * @param ec                          ExecutionContext
    * @tparam K Type of key
    * @tparam V Type of value
    * @return Database instance
    */

  def apply[K, V](dir: Path,
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
                  acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                     valueSerializer: Serializer[V],
                                                                                     keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                     ec: ExecutionContext = SwayDB.defaultExecutionContext): Try[Map[K, V]] =
    CoreAPI(
      config = DefaultPersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        mapSize = mapSize, mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        recoveryMode = recoveryMode,
        mmapAppendix = mmapAppendix,
        segmentSize = segmentSize,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = lastLevelGroupingStrategy,
        acceleration = acceleration
      ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        swaydb.Map[K, V](new SwayDB(core))
    }
}
