package swaydb.core.segment.format.a.entry.reader.value

import scala.util.Try
import swaydb.data.slice.{Reader, Slice}

object LazyPutValueReader {
  def apply(reader: Reader,
            offset: Int,
            length: Int): LazyGroupValueReader =
    new LazyGroupValueReader {
      override val valueReader: Reader = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

trait LazyPutValueReader extends LazyValueReader {

  override def getOrFetchValue: Try[Option[Slice[Byte]]] =
    super.getOrFetchValue

  override def isValueDefined: Boolean =
    super.isValueDefined
}
