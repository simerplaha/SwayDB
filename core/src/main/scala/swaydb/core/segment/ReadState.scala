package swaydb.core.segment

import java.nio.file.Path

sealed trait ReadState {
  def isSequential(path: Path): Boolean
  def setSequential(path: Path, isSequential: Boolean): Unit
}

object ReadState {

  def apply(): ReadState =
    new Some(new java.util.HashMap[Path, Boolean]())

  class Some(map: java.util.HashMap[Path, Boolean]) extends ReadState {

    def isSequential(path: Path): Boolean = {
      val isSeq = map.get(path)
      isSeq == null || isSeq
    }

    def setSequential(path: Path, isSequential: Boolean): Unit =
      map.put(path, isSequential)
  }

  object Random extends ReadState {
    def isSequential(path: Path): Boolean =
      false

    def setSequential(path: Path, isSequential: Boolean): Unit =
      ()
  }

  object Sequential extends ReadState {
    def isSequential(path: Path): Boolean =
      true

    def setSequential(path: Path, isSequential: Boolean): Unit =
      ()
  }

  def random: ReadState =
    if (scala.util.Random.nextBoolean())
      ReadState()
    else if (scala.util.Random.nextBoolean())
      Random
    else
      Sequential

}

