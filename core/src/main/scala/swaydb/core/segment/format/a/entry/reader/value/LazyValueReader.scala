package swaydb.core.segment.format.a.entry.reader.value

import scala.util.{Success, Try}
import swaydb.core.segment.format.a.SegmentReader
import swaydb.data.slice.{Reader, Slice}

object LazyValueReader {
  def apply(reader: Reader,
            offset: Int,
            length: Int): LazyValueReader =
    new LazyValueReader {
      override val valueReader: Reader = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

trait LazyValueReader {

  @volatile var valueOption: Option[Slice[Byte]] = _

  def valueReader: Reader

  def valueLength: Int

  def valueOffset: Int

  //tries fetching the value from the given reader
  private def fetchValue(reader: Reader): Try[Option[Slice[Byte]]] =
    if (valueOption == null)
      SegmentReader.readBytes(valueOffset, valueLength, reader) map {
        value =>
          valueOption = value
          value
      }
    else
      Success(valueOption)

  def getOrFetchValue: Try[Option[Slice[Byte]]] =
    fetchValue(valueReader)

  def isValueDefined: Boolean =
    valueOption != null

  def getValue: Option[Slice[Byte]] =
    valueOption

  override def equals(that: Any): Boolean =
    that match {
      case other: LazyValueReader =>
        getOrFetchValue == other.getOrFetchValue

      case _ =>
        false
    }

}
