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
import swaydb.{Set, SwayDB}

object Set extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    */
  def memorySet[T](mapSize: Int = 4.mb,
                   segmentSize: Int = 2.mb,
                   cacheSize: Int = 500.mb, //cacheSize for memory database is used for evicting decompressed key-values
                   cacheCheckDelay: FiniteDuration = 7.seconds,
                   bloomFilterFalsePositiveRate: Double = 0.01,
                   compressDuplicateValues: Boolean = false,
                   groupingStrategy: Option[KeyValueGroupingStrategy] = None,
                   acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                      keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                      ec: ExecutionContext = SwayDB.defaultExecutionContext): Try[Set[T]] =
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
        swaydb.Set[T](new SwayDB(core))
    }
}
