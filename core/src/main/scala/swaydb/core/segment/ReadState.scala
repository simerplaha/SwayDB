package swaydb.core.segment

import java.nio.file.Path

import swaydb.core.util.HashSlot

sealed trait ReadState {
  def isSequential(path: Path): Boolean
  def setSequential(path: Path, isSequential: Boolean): Unit
}

object ReadState {

  def hashMap(): ReadState =
    new HashMapState(new java.util.HashMap[Path, Boolean]())

  def hashSlot(slots: Int): ReadState =
    new SlotState(new HashSlot[Path, Boolean](new Array[Boolean](slots)))

  class HashMapState(map: java.util.HashMap[Path, Boolean]) extends ReadState {

    def isSequential(path: Path): Boolean = {
      val isSeq = map.get(path)
      isSeq == null || isSeq
    }

    def setSequential(path: Path, isSequential: Boolean): Unit =
      map.put(path, isSequential)
  }

  class SlotState(map: HashSlot[Path, Boolean]) extends ReadState {

    def isSequential(path: Path): Boolean =
      map.get(path).forall(_ == true)

    def setSequential(path: Path, isSequential: Boolean): Unit =
      map.put(path, isSequential)

    override def toString: String =
      map.toString
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
      ReadState.hashMap()
    else if (scala.util.Random.nextBoolean())
      Random
    else
      Sequential

}

