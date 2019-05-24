package swaydb.core.level.compaction

sealed trait LevelState
object LevelState {
  case object Idle extends LevelState
}
