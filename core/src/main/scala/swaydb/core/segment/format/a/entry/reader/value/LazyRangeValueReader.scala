package swaydb.core.segment.format.a.entry.reader.value

import scala.util.{Failure, Success, Try}
import swaydb.core.data.Value
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.data.slice.Reader

object LazyRangeValueReader {

  def apply(reader: Reader,
            offset: Int,
            length: Int): LazyRangeValueReader =
    new LazyRangeValueReader {
      override val valueReader: Reader = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

//GAH! Inheritance Yuk! Need to update this code.
trait LazyRangeValueReader extends LazyValueReader {

  @volatile private var fromValue: Option[Value.FromValue] = _
  @volatile private var rangeValue: Value.RangeValue = _

  def fetchRangeValue: Try[Value.RangeValue] =
    if (rangeValue == null)
      fetchFromAndRangeValue.map(_._2)
    else
      Success(rangeValue)

  def fetchFromValue: Try[Option[Value.FromValue]] =
    if (fromValue == null)
      fetchFromAndRangeValue.map(_._1)
    else
      Success(fromValue)

  def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)] =
    if (fromValue == null || rangeValue == null)
      getOrFetchValue flatMap {
        case Some(fromAndRangeValueBytes) =>
          RangeValueSerializer.read(fromAndRangeValueBytes) map {
            case (fromValue, rangeValue) =>
              this.fromValue = fromValue.map(_.unslice)
              this.rangeValue = rangeValue.unslice
              (this.fromValue, this.rangeValue)
          }
        case None =>
          Failure(new IllegalStateException(s"Failed to read range's value"))
      }
    else
      Success(fromValue, rangeValue)
}
