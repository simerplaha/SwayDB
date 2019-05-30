package swaydb.core.level.compaction

import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef, TrashLevel}

object CompactionOrdering {

  val ordering = new Ordering[LevelRef] {
    override def compare(left: LevelRef, right: LevelRef): Int =
      (left, right) match {
        //Level
        case (left: Level, right: Level) => order(left, right)
        case (left: Level, right: LevelZero) => order(left, right)
        case (left: Level, right: TrashLevel.type) => order(left, right)
        //LevelZero
        case (left: LevelZero, right: Level) => order(left, right)
        case (left: LevelZero, right: LevelZero) => order(left, right)
        case (left: LevelZero, right: TrashLevel.type) => order(left, right)
        //LevelZero
        case (left: TrashLevel.type, right: Level) => order(left, right)
        case (left: TrashLevel.type, right: LevelZero) => order(left, right)
        case (left: TrashLevel.type, right: TrashLevel.type) => order(left, right)
      }
  }

  def order(left: Level, right: TrashLevel.type): Int =
    1

  def order(left: TrashLevel.type, right: Level): Int =
    -1

  def order(left: TrashLevel.type, right: TrashLevel.type): Int =
    0

  def order(left: TrashLevel.type, right: LevelZero): Int =
    -1

  def order(left: LevelZero, right: TrashLevel.type): Int =
    1

  def order(left: LevelZero, right: LevelZero): Int =
    0

  def order(left: LevelZero, right: Level): Int =
    if (left.level0Meter.mapsCount >= 4)
      1
    else
      -1

  def order(left: Level, right: LevelZero): Int =
    order(right, left) * -1

  def order(left: Level, right: Level): Int =
    if (right.nextLevel.isEmpty) //last Level is always the lowest priority. TODO - check for expired keys
      1
    else
      left.throttle(left.meter).pushDelay compareTo right.throttle(right.meter).pushDelay
}

