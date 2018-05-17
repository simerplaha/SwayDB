/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import java.nio.file.Path

import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.Throttle
import swaydb.data.config._

import scala.concurrent.duration._

object PersistentConfig {

  /**
    * Default configuration for a persistent 8 Leveled database.
    */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            mapSize: Int,
            mmapMaps: Boolean,
            recoveryMode: RecoveryMode,
            mmapSegments: MMAP,
            mmapAppendix: Boolean,
            segmentSize: Int,
            appendixFlushCheckpointSize: Int,
            bloomFilterFalsePositiveRate: Double,
            minTimeLeftToUpdateExpiration: FiniteDuration,
            acceleration: Level0Meter => Accelerator): SwayDBPersistentConfig =
    ConfigWizard
      .addPersistentLevel0(
        dir = dir,
        mapSize = mapSize,
        mmap = mmapMaps,
        recoveryMode = recoveryMode,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        acceleration = acceleration
      )
      .addPersistentLevel1(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = true,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        throttle =
          levelMeter => {
            val delay = (10 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 10
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = true,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        throttle =
          levelMeter => {
            val delay = (5 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 10
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = true,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        throttle =
          levelMeter => {
            val delay = (5 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 10
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        throttle =
          levelMeter => {
            val delay = (30 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 10
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        throttle =
          levelMeter => {
            val delay = (40 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 10
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        throttle =
          levelMeter => {
            val delay = (50 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 10
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        throttle =
          levelMeter => {
            val delay = (10 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 10
            Throttle(delay, batch)
          }
      )
}
