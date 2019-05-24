package swaydb.core.level.compaction

import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero


sealed trait Job
sealed trait TemporaryJob extends Job
object Job {
  case class Zero(zero: LevelZero, nextLevel: Level) extends Job
  case class Push(level: Level) extends Job
  case class CompactSmallSegments(level: Level) extends TemporaryJob
  case class ClearExpiredKeyValues(level: Level) extends TemporaryJob
}
