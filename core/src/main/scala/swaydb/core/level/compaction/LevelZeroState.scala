package swaydb.core.level.compaction

sealed trait LevelZeroState
object LevelZeroState {
  case object Idle extends LevelZeroState
}
