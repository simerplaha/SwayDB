package swaydb.core.level.compaction

sealed trait CompactionJob
object CompactionJob {
  case object LevelZero extends CompactionJob
  case class NextLevel(level: NextLevel) extends CompactionJob
  case object Expire extends CompactionJob
  case object Collapse extends CompactionJob
}