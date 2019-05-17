package swaydb.core.level.actor

object LevelMerger {

  case class State(states: Seq[LevelState],
                   concurrency: Int)

  def update(level: Int, segments: Int, allowedMeter: Double)(implicit state: State) =
    ???
}
