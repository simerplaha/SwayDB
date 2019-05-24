package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.zero.LevelZero

import scala.concurrent.ExecutionContext

object LeveledCompaction {

  def apply(levelZero: LevelZero)(implicit ec: ExecutionContext): WiredActor[LeveledCompaction] =
    WiredActor(new LeveledCompaction(levelZero))

}

class LeveledCompaction(levelZero: LevelZero) {

}
