package swaydb.core.segment.format.a.entry.reader.value

import scala.util.{Failure, Success, Try}
import swaydb.core.data.Value
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.ValueSerializer
import swaydb.data.slice.{Reader, Slice}

object LazyPendingApplyValueReader {
  def apply(reader: Reader,
            offset: Int,
            length: Int): LazyPendingApplyValueReader =
    new LazyPendingApplyValueReader {
      override def valueReader: Reader = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

//GAH! Inheritance Yuk! Need to update this code.
trait LazyPendingApplyValueReader extends LazyValueReader {

  @volatile private var applyFunctions: Slice[Value.Apply] = _

  def getOrFetchApplies: Try[Slice[Value.Apply]] =
    if (applyFunctions == null)
      getOrFetchValue flatMap {
        case Some(valueBytes) =>
          ValueSerializer.read[Slice[Value.Apply]](valueBytes) map {
            applies =>
              this.applyFunctions = applies.map(_.unslice)
              applyFunctions
          }
        case None =>
          Failure(new IllegalStateException(s"Failed to read ApplyValue's value"))
      }
    else
      Success(applyFunctions)
}

object ActivePendingApplyValueReader {
  def apply(applies: Slice[Value.Apply]): ActivePendingApplyValueReader =
    new ActivePendingApplyValueReader(applies)
}

class ActivePendingApplyValueReader(applies: Slice[Value.Apply]) extends LazyPendingApplyValueReader {

  override def getOrFetchApplies: Try[Slice[Value.Apply]] = Success(applies)

  override def valueReader: Reader = {
    val bytesRequires = ValueSerializer.bytesRequired(applies)
    val slice = Slice.create[Byte](bytesRequires)
    ValueSerializer.write(applies)(slice)
    Reader(slice)
  }

  override def valueLength: Int =
    ValueSerializer.bytesRequired(applies)

  override def valueOffset: Int =
    0
}
