package swaydb.core.segment.format.a.entry.reader.base

import scala.util.Try
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.slice.Reader

trait BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[Try[T]]

}
