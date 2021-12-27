package swaydb.core.file.sweeper

import java.nio.file.Path

private[core] trait FileSweeperItem {
  def path: Path
  def delete(): Unit
  def close(): Unit
  def isOpen(): Boolean
}
