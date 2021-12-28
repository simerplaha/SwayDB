package swaydb.core.file.sweeper

import java.nio.file.Path

private[core] sealed trait FileSweeperItem {
  def path: Path
  def isOpen(): Boolean
}

private[core] object FileSweeperItem {

  trait Closeable extends FileSweeperItem {
    def close(): Unit
  }

  trait Deletable extends FileSweeperItem {
    def delete(): Unit
  }

}
