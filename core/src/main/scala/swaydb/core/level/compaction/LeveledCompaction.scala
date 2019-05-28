package swaydb.core.level.compaction

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.WiredActor
import swaydb.core.data.Memory
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.Map
import swaydb.data.IO
import swaydb.data.slice.Slice

import scala.concurrent.ExecutionContext

private[core] object LeveledCompaction extends LazyLogging {

  class State(val levelZero: LevelZero,
              val nextLevel: Level,
              val maxConcurrentCompactions: Int,
              var runningCompactions: Int)

  def create(levelZero: LevelZero,
             maxConcurrentCompactions: Int)(implicit ec: ExecutionContext): Option[WiredActor[LeveledCompaction.type, State]] = {
    //temporarily do typecast. It should actually return a CompactionAPI type.
    val levels = Level.getLevels(levelZero).drop(1) map (_.asInstanceOf[Level])
    levels.headOption map {
      nextLevel =>
        WiredActor(
          impl = LeveledCompaction,
          state =
            new State(
              levelZero = levelZero,
              nextLevel = nextLevel,
              maxConcurrentCompactions = maxConcurrentCompactions,
              runningCompactions = 0
            )
        )
    }
  }

  private[compaction] def put(map: Map[Slice[Byte], Memory.SegmentResponse],
                              level: Level,
                              state: State): IO[Unit] =
    ???

  def startCompaction(state: State) = {
    ???
  }
}

