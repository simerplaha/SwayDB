package swaydb.core.level

import swaydb.core.data.Memory
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.data.IO
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

object NextLevel {

  def foreachRight[T](level: NextLevel, f: NextLevel => T): Unit = {
    level.nextLevel foreach {
      nextLevel =>
        foreachRight(nextLevel, f)
    }
    f(level)
  }

  def reverseNextLevels(level: NextLevel): ListBuffer[NextLevel] = {
    val levels = ListBuffer.empty[NextLevel]
    NextLevel.foreachRight(
      level = level,
      f = level =>
        levels += level
    )
    levels
  }
}

/**
  * Levels that can have upper Levels or Levels that upper Levels can merge Segments or Maps into.
  */
trait NextLevel extends LevelRef {

  def isUnReserved(minKey: Slice[Byte], maxKey: Slice[Byte]): Boolean

  def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte]): Boolean

  def isCopyable(map: Map[Slice[Byte], Memory.SegmentResponse]): Boolean

  def partitionUnreservedCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment])

  def put(segment: Segment): IO.Async[Unit]

  def put(map: Map[Slice[Byte], Memory.SegmentResponse]): IO.Async[Unit]

  def put(segments: Iterable[Segment]): IO.Async[Unit]

  def removeSegments(segments: Iterable[Segment]): IO[Int]

  def meter: LevelMeter

  def refresh(segment: Segment): IO.Async[Unit]

  def collapse(segments: Iterable[Segment]): IO.Async[Int]

  def reverseNextLevels: ListBuffer[NextLevel] = {
    val levels = ListBuffer.empty[NextLevel]
    NextLevel.foreachRight(
      level = this,
      f = level =>
        levels += level
    )
    levels
  }
}
