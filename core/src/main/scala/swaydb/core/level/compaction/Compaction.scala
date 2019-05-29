package swaydb.core.level.compaction

import swaydb.core.level.{LevelRef, NextLevel}
import swaydb.data.IO
import swaydb.data.IO._

object Compaction {

  val leveledCompactionOrdering = new Ordering[LevelRef] {
    override def compare(left: LevelRef, right: LevelRef): Int =
      ???
  }

  def tryCopyForward(level: NextLevel): IO[Unit] =
    level.nextLevel map {
      nextLevel =>
        val copyableSegments = nextLevel.partitionUnreservedCopyable(level.segmentsInLevel())._1
        if (copyableSegments.isEmpty)
          IO.unit
        else
          nextLevel.put(copyableSegments) match {
            case IO.Success(_) =>
              level.removeSegments(copyableSegments) map (_ => ())

            case IO.Later(_, _) | IO.Failure(_) =>
              IO.unit
          }
    } getOrElse IO.unit

  def tryCopyForwardAll(level: NextLevel): IO[Unit] =
    level.reverseNextLevels.foreachIO[Unit](
      failFast = false,
      f = tryCopyForward
    ) getOrElse IO.unit
}
