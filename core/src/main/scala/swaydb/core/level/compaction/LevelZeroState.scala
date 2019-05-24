package swaydb.core.level.compaction

sealed trait LevelZeroState
object LevelZeroState {
  case object Idle extends LevelZeroState
  case object Ready extends LevelZeroState
  case object Pushing extends LevelZeroState
  case object WaitingPull extends LevelZeroState
}
