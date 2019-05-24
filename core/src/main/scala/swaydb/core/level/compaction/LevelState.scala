package swaydb.core.level.compaction

import swaydb.core.segment.Segment

sealed trait LevelState

object LevelState {
  sealed trait HasBusySegments extends LevelState {
    def busySegments: Seq[Segment]
  }

  case object Idle extends LevelState
  case class CollapsingSegments(segments: Seq[Segment]) extends HasBusySegments {
    override def busySegments: Seq[Segment] = segments
  }
  case class ClearingExpiredKeyValues(segment: Segment) extends HasBusySegments {
    override def busySegments: Seq[Segment] = Seq(segment)
  }
}
