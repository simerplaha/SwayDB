package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try
import swaydb.configs.level.DefaultMemoryConfig
import swaydb.core.CoreAPI
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Map, SwayDB}

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * A 2 Leveled (Level0 & Level1), in-memory database.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    *
    * @param mapSize         size of Level0 maps before they are converted into Segments
    * @param segmentSize     size of Level1 Segments
    * @param acceleration    Controls the write speed.
    * @param keySerializer   Converts keys to Bytes
    * @param valueSerializer Converts values to Bytes
    * @param ordering        Sort order for keys
    * @param ec
    * @tparam K
    * @tparam V
    * @return
    */

  def apply[K, V](mapSize: Int = 4.mb,
                  segmentSize: Int = 2.mb,
                  cacheSize: Int = 500.mb,
                  cacheCheckDelay: FiniteDuration = 7.seconds,
                  bloomFilterFalsePositiveRate: Double = 0.01,
                  compressDuplicateValues: Boolean = false,
                  groupingStrategy: Option[KeyValueGroupingStrategy] = None,
                  acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                     valueSerializer: Serializer[V],
                                                                                     keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                     ec: ExecutionContext = SwayDB.defaultExecutionContext): Try[Map[K, V]] =
    CoreAPI(
      config = DefaultMemoryConfig(
        mapSize = mapSize,
        segmentSize = segmentSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = groupingStrategy,
        acceleration = acceleration
      ),
      maxOpenSegments = 0,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      //memory Segments are never closed.
      segmentsOpenCheckDelay = Duration.Zero
    ) map {
      core =>
        swaydb.Map[K, V](new SwayDB(core))
    }

}
