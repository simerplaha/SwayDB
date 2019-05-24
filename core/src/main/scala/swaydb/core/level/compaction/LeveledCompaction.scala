package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero

import scala.collection.mutable
import scala.concurrent.ExecutionContext

private[core] object LeveledCompaction {

  private[core] class State(jobs: Seq[Job],
                            levelZeroState: LevelZeroState,
                            levelState: mutable.Map[Level, LevelState])

  def create(levelZero: LevelZero)(implicit ec: ExecutionContext): Option[WiredActor[LeveledCompaction.type, State]] = {
    //temporarily do typecast. It should actually return a CompactionAPI type.
    val levels = Level.getLevels(levelZero) map (_.asInstanceOf[Level])
    levels.headOption map {
      nextLevel =>
        val jobZero = Job.Zero(levelZero, nextLevel)
        val otherJobs =
          levels flatMap {
            level =>
              Seq(
                Job.Push(level),
                Job.CompactSmallSegments(level),
                Job.ClearExpiredKeyValues(level)
              )
          }
        val allJobs = jobZero +: otherJobs

        val levelStates: mutable.Map[Level, LevelState] =
          levels.map(level => level -> LevelState.Idle)(collection.breakOut)

        WiredActor(
          impl = LeveledCompaction,
          state =
            new State(
              jobs = allJobs,
              levelZeroState = LevelZeroState.Idle,
              levelState = levelStates
            )
        )
    }
  }

  def startCompaction(state: State) =
    ???

}

