package swaydb.core.segment.format.a.entry.reader.value

import scala.util.{Failure, Success, Try}
import swaydb.data.slice.{Reader, Slice}

object LazyFunctionReader {
  def apply(reader: Reader,
            offset: Int,
            length: Int): LazyFunctionReader =
    new LazyFunctionReader {
      override val valueReader: Reader = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

trait LazyFunctionReader extends LazyValueReader {

  def getOrFetchFunction: Try[Slice[Byte]] =
    super.getOrFetchValue flatMap {
      case Some(value) =>
        Success(value)
      case None =>
        Failure(new Exception("Empty functionId."))
    }

  override def isValueDefined: Boolean =
    super.isValueDefined
}
