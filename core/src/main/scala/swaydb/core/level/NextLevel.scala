package swaydb.core.level

import swaydb.core.data.Memory
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.data.IO
import swaydb.data.slice.Slice

/**
  * Levels that can have upper Levels or Levels that upper Levels can merge Segments or Maps into.
  */
trait NextLevel extends LevelRef {

  def isCopyable(map: Map[Slice[Byte], Memory.SegmentResponse]): Boolean

  def partitionUnreservedCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment])

  def put(segment: Segment): IO.Async[Unit]

  def put(map: Map[Slice[Byte], Memory.SegmentResponse]): IO.Async[Unit]

  def put(segments: Iterable[Segment]): IO.Async[Unit]
}
