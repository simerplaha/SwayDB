package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero

import scala.concurrent.ExecutionContext

private[core] object LeveledCompaction {

  private[core] class MutableState(levelZero: LevelZero,
                                   levels: Seq[Level])

  def create(levelZero: LevelZero)(implicit ec: ExecutionContext): WiredActor[LeveledCompaction.type, MutableState] = {
    //temporarily do typecast. It should actually return a CompactionAPI type.
    val levels = Level.getLevels(levelZero) map (_.asInstanceOf[Level])
    WiredActor(LeveledCompaction, new MutableState(levelZero, levels))
  }
}

