package swaydb.core.segment.format.a.block

private[core] trait BlockOffset {
  def start: Int
  def size: Int
  def end: Int =
    start + size - 1
}
