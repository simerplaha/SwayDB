/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import java.nio.file.Path
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.build.Build
import swaydb.core.level.tool.AppendixRepairer
import swaydb.core.sweeper.FileSweeper
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.repairAppendix.RepairResult.OverlappingSegments
import swaydb.data.repairAppendix._
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

/**
 * Instance used for creating/initialising databases.
 */
object SwayDB extends LazyLogging {

  final val version: Build.Version = Build.thisVersion()

  /**
   * Documentation: http://www.swaydb.io/api/repairAppendix
   */
  def repairAppendix[K](levelPath: Path,
                        repairStrategy: AppendixRepairStrategy)(implicit serializer: Serializer[K],
                                                                fileSweeper: FileSweeper,
                                                                keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): IO[swaydb.Error.Level, RepairResult[K]] =
  //convert to typed result.
    AppendixRepairer(levelPath, repairStrategy) match {
      case IO.Left(swaydb.Error.Fatal(OverlappingSegmentsException(segmentInfo, overlappingSegmentInfo))) =>
        IO.Right[swaydb.Error.Segment, RepairResult[K]](
          OverlappingSegments[K](
            segmentInfo =
              SegmentInfo(
                path = segmentInfo.path,
                minKey = serializer.read(segmentInfo.minKey),
                maxKey =
                  segmentInfo.maxKey match {
                    case MaxKey.Fixed(maxKey) =>
                      MaxKey.Fixed(serializer.read(maxKey))

                    case MaxKey.Range(fromKey, maxKey) =>
                      MaxKey.Range(fromKey = serializer.read(fromKey), maxKey = serializer.read(maxKey))
                  },
                segmentSize = segmentInfo.segmentSize,
                keyValueCount = segmentInfo.keyValueCount
              ),
            overlappingSegmentInfo =
              SegmentInfo(
                path = overlappingSegmentInfo.path,
                minKey = serializer.read(overlappingSegmentInfo.minKey),
                maxKey =
                  overlappingSegmentInfo.maxKey match {
                    case MaxKey.Fixed(maxKey) =>
                      MaxKey.Fixed(serializer.read(maxKey))

                    case MaxKey.Range(fromKey, maxKey) =>
                      MaxKey.Range(fromKey = serializer.read(fromKey), maxKey = serializer.read(maxKey))
                  },
                segmentSize = overlappingSegmentInfo.segmentSize,
                keyValueCount = overlappingSegmentInfo.keyValueCount
              )
          )
        )

      case IO.Left(error) =>
        IO.Left(error)

      case IO.Right(_) =>
        IO.Right[swaydb.Error.Segment, RepairResult[K]](RepairResult.Repaired)
    }
}
