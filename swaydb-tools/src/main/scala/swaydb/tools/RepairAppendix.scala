package swaydb.tools

import swaydb.IO
import swaydb.config.repairAppendix.RepairResult.OverlappingSegments
import swaydb.config.repairAppendix.{AppendixRepairStrategy, OverlappingSegmentsException, RepairResult, SegmentInfo}
import swaydb.core.sweeper.FileSweeper
import swaydb.core.tool.AppendixRepairer
import swaydb.serializers.Serializer
import swaydb.slice.{MaxKey, Slice}
import swaydb.slice.order.KeyOrder

import java.nio.file.Path

object RepairAppendix {

  /**
   * Documentation: http://www.swaydb.io/api/repairAppendix
   */
  def apply[K](levelPath: Path,
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
