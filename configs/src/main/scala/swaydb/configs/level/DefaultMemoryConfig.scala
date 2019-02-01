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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.configs.level

import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.compaction.Throttle
import swaydb.data.config._

import scala.concurrent.duration._

object DefaultMemoryConfig {

  /**
    * Default configuration for 2 leveled Memory database.
    */
  def apply(mapSize: Int,
            segmentSize: Int,
            bloomFilterFalsePositiveRate: Double,
            compressDuplicateValues: Boolean,
            deleteSegmentsEventually: Boolean,
            groupingStrategy: Option[KeyValueGroupingStrategy],
            acceleration: Level0Meter => Accelerator): SwayDBMemoryConfig =
    ConfigWizard
      .addMemoryLevel0(
        mapSize = mapSize,
        acceleration = acceleration
      )
      .addMemoryLevel1(
        segmentSize = segmentSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = groupingStrategy,
        deleteSegmentsEventually = deleteSegmentsEventually,
        throttle =
          _ =>
            Throttle(5.seconds, 5)
      )
}
