package swaydb.core.level.compaction

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef}

import scala.concurrent.ExecutionContext

private[core] object LeveledCompaction extends LazyLogging {

  class State(val levelZero: LevelZero,
              val nextLevel: Level,
              val maxConcurrentCompactions: Int,
              var runningCompactions: Int)

  def create(levelZero: LevelZero,
             maxConcurrentCompactions: Int)(implicit ec: ExecutionContext): Option[LeveledCompaction.State] = {
    //temporarily do typecast. It should actually return a CompactionAPI type.
    val levels = LevelRef.getLevels(levelZero).drop(1) map (_.asInstanceOf[Level])
    levels.headOption map {
      nextLevel =>
        new State(
          levelZero = levelZero,
          nextLevel = nextLevel,
          maxConcurrentCompactions = maxConcurrentCompactions,
          runningCompactions = 0
        )
    }
  }

  def score(state: State) = ???

  def startCompaction(state: State) = {
    ???
  }
}

