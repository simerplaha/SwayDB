package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero

import scala.concurrent.ExecutionContext

private[core] object LeveledCompaction {

  sealed trait Job
  sealed trait TemporaryJob extends Job
  object Job {
    case class Zero(zero: LevelZero, nextLevel: Level) extends Job
    case class Push(level: Level) extends Job
    case class CompactSmallSegments(level: Level) extends TemporaryJob
    case class ClearExpiredKeyValues(level: Level) extends TemporaryJob
  }

  private[core] class State(jobs: Seq[Job])

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

        WiredActor(
          impl = LeveledCompaction,
          state = new State(allJobs)
        )
    }
  }
}

