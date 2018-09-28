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
import swaydb.data.api.grouping.{Compression, KeyValueGroupingStrategy}
import swaydb.data.compaction.Throttle
import swaydb.data.compression.{LZ4Compressor, LZ4Decompressor, LZ4Instance}
import swaydb.data.config._
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

object DefaultPersistentConfig {

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
            compressDuplicateValues: Boolean,
            acceleration: Level0Meter => Accelerator): SwayDBPersistentConfig =
    ConfigWizard
      .addPersistentLevel0( //level0
        dir = dir,
        mapSize = mapSize,
        mmap = mmapMaps,
        recoveryMode = recoveryMode,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        acceleration = acceleration
      )
      .addPersistentLevel1( //level1
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = true,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = None,
        throttle =
          levelMeter => {
            val delay = (10 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level2
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = true,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = None,
        throttle =
          levelMeter => {
            val delay = (5 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level3
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = true,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = None,
        throttle =
          levelMeter => {
            val delay = (5 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level4
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = None,
        throttle =
          levelMeter => {
            val delay = (30 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level5
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy =
          Some(
            KeyValueGroupingStrategy.Size(
              size = 1.mb,
              indexCompressions =
                Seq(
                  Compression.LZ4(
                    compressor = (LZ4Instance.FastestJavaInstance, LZ4Compressor.FastCompressor(minCompressionPercentage = 15)),
                    decompressor = (LZ4Instance.FastestJavaInstance, LZ4Decompressor.FastDecompressor)
                  ),
                  Compression.UnCompressedGroup
                ),
              valueCompressions =
                Seq(
                  Compression.LZ4(
                    compressor = (LZ4Instance.FastestJavaInstance, LZ4Compressor.FastCompressor(minCompressionPercentage = 15)),
                    decompressor = (LZ4Instance.FastestJavaInstance, LZ4Decompressor.FastDecompressor)
                  ),
                  Compression.UnCompressedGroup
                ),
              groupGroupingStrategy = None
            )
          ),
        throttle =
          levelMeter => {
            val delay = (40 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level6
        dir = dir,
        otherDirs = otherDirs,
        //double the size in last Levels so that if merge is not triggered(copied Segment),
        // small Segment check will merge the segment into one of the other Segments and apply compression
        segmentSize = segmentSize * 2,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = Some(DefaultGroupingStrategy()),
        throttle =
          levelMeter => {
            val delay = (50 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level7
        dir = dir,
        otherDirs = otherDirs,
        //double the size in last Levels so that if merge is not triggered(copied Segment),
        // small Segment check will merge the segment into one of the other Segments and apply compression
        segmentSize = segmentSize * 2,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = Some(DefaultGroupingStrategy()),
        throttle =
          levelMeter => {
            val delay = (10 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
}
