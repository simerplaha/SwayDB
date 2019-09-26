/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map.serializer

import swaydb.Error.IO.ExceptionHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.data.{Time, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.ValueSerializer.SeqOfBytesSerializer
import swaydb.core.util.Bytes
import swaydb.core.util.Times._
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.implicitNotFound
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline

@implicitNotFound("Type class implementation not found for ValueSerializer of type ${T}")
sealed trait ValueSerializer[T] {

  def write(value: T, bytes: Slice[Byte]): Unit

  def read(reader: ReaderBase): T

  def read(bytes: Slice[Byte]): T =
    read(Reader(bytes))

  def bytesRequired(value: T): Int
}

object ValueSerializer {

  def readDeadline(reader: ReaderBase): Option[Deadline] = {
    val deadline = reader.readUnsignedLong()
    if (deadline == 0)
      None
    else
      deadline.toDeadlineOption
  }

  def readTime(reader: ReaderBase): Time = {
    val timeSize = reader.readUnsignedInt()
    if (timeSize == 0)
      Time.empty
    else
      Time(reader.read(timeSize))
  }

  def readRemainingTime(reader: ReaderBase): Time = {
    val remaining = reader.readRemaining()
    if (remaining.isEmpty)
      Time.empty
    else
      Time(remaining)
  }

  def readValue(reader: ReaderBase): Option[Slice[Byte]] = {
    val remaining = reader.readRemaining()
    if (remaining.isEmpty)
      None
    else
      Some(remaining)
  }

  implicit object ValuePutSerializer extends ValueSerializer[Value.Put] {

    override def write(value: Value.Put, bytes: Slice[Byte]): Unit =
      bytes
        .addUnsignedLong(value.deadline.toNanos)
        .addUnsignedInt(value.time.size)
        .addAll(value.time.time)
        .addAll(value.value.getOrElse(Slice.emptyBytes))

    override def bytesRequired(value: Value.Put): Int =
      Bytes.sizeOfUnsignedLong(value.deadline.toNanos) +
        Bytes.sizeOfUnsignedInt(value.time.size) +
        value.time.size +
        value.value.map(_.size).getOrElse(0)

    override def read(reader: ReaderBase): Value.Put = {
      val deadline = readDeadline(reader)
      val time = readTime(reader)
      val value = readValue(reader)
      Value.Put(value, deadline, time)
    }
  }

  implicit object ValueUpdateSerializer extends ValueSerializer[Value.Update] {

    override def write(value: Value.Update, bytes: Slice[Byte]): Unit =
      bytes
        .addUnsignedLong(value.deadline.toNanos)
        .addUnsignedInt(value.time.size)
        .addAll(value.time.time)
        .addAll(value.value.getOrElse(Slice.emptyBytes))

    override def bytesRequired(value: Value.Update): Int =
      Bytes.sizeOfUnsignedLong(value.deadline.toNanos) +
        Bytes.sizeOfUnsignedInt(value.time.size) +
        value.time.size +
        value.value.map(_.size).getOrElse(0)

    override def read(reader: ReaderBase): Value.Update = {
      val deadline = readDeadline(reader)
      val time = readTime(reader)
      val value = readValue(reader)
      Value.Update(value, deadline, time)
    }
  }

  implicit object ValueRemoveSerializer extends ValueSerializer[Value.Remove] {

    override def write(value: Value.Remove, bytes: Slice[Byte]): Unit =
      bytes
        .addUnsignedLong(value.deadline.toNanos)
        .addAll(value.time.time)

    override def bytesRequired(value: Value.Remove): Int =
      Bytes.sizeOfUnsignedLong(value.deadline.toNanos) +
        value.time.size

    override def read(reader: ReaderBase): Value.Remove = {
      val deadline = readDeadline(reader)
      val time = readRemainingTime(reader)
      Value.Remove(deadline, time)
    }
  }

  implicit object ValueFunctionSerializer extends ValueSerializer[Value.Function] {
    override def write(value: Value.Function, bytes: Slice[Byte]): Unit =
      ValueSerializer.write((value.function, value.time.time))(bytes)(TupleOfBytesSerializer)

    override def bytesRequired(value: Value.Function): Int =
      ValueSerializer.bytesRequired((value.function, value.time.time))(TupleOfBytesSerializer)

    override def read(reader: ReaderBase): Value.Function = {
      val (function, time) = ValueSerializer.read[(Slice[Byte], Slice[Byte])](reader)
      Value.Function(function, Time(time))
    }
  }

  implicit object ValueSliceApplySerializer extends ValueSerializer[Slice[Value.Apply]] {

    override def write(applies: Slice[Value.Apply], bytes: Slice[Byte]): Unit = {
      bytes.addUnsignedInt(applies.size)
      applies foreach {
        case value: Value.Update =>
          val bytesRequired = ValueSerializer.bytesRequired(value)
          ValueSerializer.write(value)(bytes.addUnsignedInt(0).addUnsignedInt(bytesRequired))

        case value: Value.Function =>
          val bytesRequired = ValueSerializer.bytesRequired(value)
          ValueSerializer.write(value)(bytes.addUnsignedInt(1).addUnsignedInt(bytesRequired))

        case value: Value.Remove =>
          val bytesRequired = ValueSerializer.bytesRequired(value)
          ValueSerializer.write(value)(bytes.addUnsignedInt(2).addUnsignedInt(bytesRequired))
      }
    }

    override def bytesRequired(value: Slice[Value.Apply]): Int =
    //also add the total number of entries.
      value.foldLeft(Bytes.sizeOfUnsignedInt(value.size)) {
        case (total, function) =>
          function match {
            case value: Value.Update =>
              val bytesRequired = ValueSerializer.bytesRequired(value)
              total + Bytes.sizeOfUnsignedInt(0) + Bytes.sizeOfUnsignedInt(bytesRequired) + bytesRequired

            case value: Value.Function =>
              val bytesRequired = ValueSerializer.bytesRequired(value)
              total + Bytes.sizeOfUnsignedInt(1) + Bytes.sizeOfUnsignedInt(bytesRequired) + bytesRequired

            case value: Value.Remove =>
              val bytesRequired = ValueSerializer.bytesRequired(value)
              total + Bytes.sizeOfUnsignedInt(2) + Bytes.sizeOfUnsignedInt(bytesRequired) + bytesRequired
          }
      }

    override def read(reader: ReaderBase): Slice[Value.Apply] = {
      val count = reader.readUnsignedInt()
      reader.foldLeft(Slice.create[Value.Apply](count)) {
        case (applies, reader) =>
          val id = reader.readUnsignedInt()
          val bytes = reader.readUnsignedIntBytes()
          if (id == 0) {
            val update = ValueSerializer.read[Value.Update](Reader(bytes))
            applies add update
            applies
          } else if (id == 1) {
            val update = ValueSerializer.read[Value.Function](Reader(bytes))
            applies add update
            applies
          } else if (id == 2) {
            val update = ValueSerializer.read[Value.Remove](Reader(bytes))
            applies add update
            applies
          }
          else
            throw IO.throwableFatal(s"Invalid id:$id")
      }
    }
  }

  implicit object ValuePendingApplySerializer extends ValueSerializer[Value.PendingApply] {

    override def write(value: Value.PendingApply, bytes: Slice[Byte]): Unit =
      ValueSerializer.write(value.applies)(bytes)

    override def bytesRequired(value: Value.PendingApply): Int =
      ValueSerializer.bytesRequired(value.applies)

    override def read(reader: ReaderBase): Value.PendingApply =
      Value.PendingApply(ValueSerializer.read[Slice[Value.Apply]](reader))
  }

  /**
   * Serializer for a tuple of Option bytes and sequence bytes.
   */
  implicit object SeqOfBytesSerializer extends ValueSerializer[Seq[Slice[Byte]]] {

    override def write(values: Seq[Slice[Byte]], bytes: Slice[Byte]): Unit =
      values foreach {
        value =>
          bytes
            .addUnsignedInt(value.size)
            .addAll(value)
      }

    override def bytesRequired(values: Seq[Slice[Byte]]): Int =
      values.foldLeft(0) {
        case (size, valueBytes) =>
          size + Bytes.sizeOfUnsignedInt(valueBytes.size) + valueBytes.size
      }

    override def read(reader: ReaderBase): Seq[Slice[Byte]] =
      reader.foldLeft(ListBuffer.empty[Slice[Byte]]) {
        case (result, reader) =>
          val size = reader.readUnsignedInt()
          val bytes = reader.read(size)
          result += bytes
      }
  }

  /**
   * Serializer for a tuple of Option bytes and sequence bytes.
   */
  implicit object TupleOfBytesSerializer extends ValueSerializer[(Slice[Byte], Slice[Byte])] {

    override def write(value: (Slice[Byte], Slice[Byte]), bytes: Slice[Byte]): Unit =
      SeqOfBytesSerializer.write(Seq(value._1, value._2), bytes)

    override def bytesRequired(value: (Slice[Byte], Slice[Byte])): Int =
      SeqOfBytesSerializer.bytesRequired(Seq(value._1, value._2))

    override def read(reader: ReaderBase): (Slice[Byte], Slice[Byte]) = {
      val bytes = SeqOfBytesSerializer.read(reader)
      if (bytes.size != 2)
        throw IO.throwableFatal(TupleOfBytesSerializer.getClass.getSimpleName + s".read did not return a tuple. Size = ${bytes.size}")
      else
        (bytes.head, bytes.last)
    }
  }

  /**
   * Serializer for a tuple of Option bytes and sequence bytes.
   */
  implicit object TupleBytesAndOptionBytesSerializer extends ValueSerializer[(Slice[Byte], Option[Slice[Byte]])] {

    override def write(value: (Slice[Byte], Option[Slice[Byte]]), bytes: Slice[Byte]): Unit =
      value._2 match {
        case Some(second) =>
          bytes.addSignedInt(1)
          ValueSerializer.write[(Slice[Byte], Slice[Byte])]((value._1, second))(bytes)
        case None =>
          bytes.addSignedInt(0)
          bytes.addAll(value._1)
      }

    override def bytesRequired(value: (Slice[Byte], Option[Slice[Byte]])): Int =
      value._2 match {
        case Some(second) =>
          1 +
            ValueSerializer.bytesRequired[(Slice[Byte], Slice[Byte])](value._1, second)
        case None =>
          1 +
            value._1.size
      }

    override def read(reader: ReaderBase): (Slice[Byte], Option[Slice[Byte]]) = {
      val id = reader.readUnsignedInt()
      if (id == 0) {
        val all = reader.readRemaining()
        (all, None)
      }
      else {
        val (left, right) = ValueSerializer.read[(Slice[Byte], Slice[Byte])](reader)
        (left, Some(right))
      }
    }
  }

  /**
   * Serializer for a tuple of Option bytes and sequence bytes.
   */
  implicit object IntMapListBufferSerializer extends ValueSerializer[mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]]] {
    val formatId = 0.toByte

    override def write(map: mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]], bytes: Slice[Byte]): Unit = {
      bytes add formatId
      map foreach {
        case (int, tuples) =>
          bytes addUnsignedInt int
          bytes addUnsignedInt tuples.size
          tuples foreach {
            case (left, right) =>
              bytes addUnsignedInt left.size
              bytes addAll left
              bytes addUnsignedInt right.size
              bytes addAll right
          }
      }
    }

    override def read(reader: ReaderBase): mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]] = {
      val format = reader.get()
      if (format != formatId)
        throw IO.throwableFatal(s"Invalid formatID: $format")
      else
        reader.foldLeft(mutable.Map.empty[Int, Iterable[(Slice[Byte], Slice[Byte])]]) {
          case (map, reader) =>
            val int = reader.readUnsignedInt()
            val tuplesCount = reader.readUnsignedInt()
            val tuples =
              (1 to tuplesCount) map {
                _ => {
                  val leftSize = reader.readUnsignedInt()
                  val left = reader.read(leftSize)
                  val rightSize = reader.readUnsignedInt()
                  val right = reader.read(rightSize)
                  (left, right)
                }
              }

            map.put(int, tuples)
            map
        }
    }

    /**
     * Calculates the number of bytes required with minimal information about the RangeFilter.
     */
    def optimalBytesRequired(numberOfRanges: Int,
                             maxUncommonBytesToStore: Int,
                             rangeFilterCommonPrefixes: Iterable[Int]): Int =
      ByteSizeOf.byte + //formatId
        rangeFilterCommonPrefixes.foldLeft(0)(_ + Bytes.sizeOfUnsignedInt(_)) + //common prefix bytes sizes
        //Bytes.sizeOf(numberOfRanges) because there can only be a max of numberOfRanges per group so ByteSizeOf.int is not required.
        (Bytes.sizeOfUnsignedInt(numberOfRanges) * rangeFilterCommonPrefixes.size) + //tuples count per common prefix count
        (numberOfRanges * Bytes.sizeOfUnsignedInt(maxUncommonBytesToStore) * 2) +
        (numberOfRanges * maxUncommonBytesToStore * 2) //store the bytes itself, * 2 because it's a tuple.

    /**
     * This is not currently used by RangeFilter, [[optimalBytesRequired]] is used instead
     * for faster calculation without long iterations. The size is almost always accurate and very rarely adds a few extra bytes.
     * See tests.
     */
    override def bytesRequired(map: mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]]): Int =
      map.foldLeft(ByteSizeOf.byte) {
        case (totalSize, (int, tuples)) =>
          Bytes.sizeOfUnsignedInt(int) +
            Bytes.sizeOfUnsignedInt(tuples.size) +
            tuples.foldLeft(0) {
              case (totalSize, (left, right)) =>
                Bytes.sizeOfUnsignedInt(left.size) +
                  left.size +
                  Bytes.sizeOfUnsignedInt(right.size) +
                  right.size +
                  totalSize
            } + totalSize
      }
  }

  def writeBytes[T](value: T)(implicit serializer: ValueSerializer[T]): Slice[Byte] = {
    val bytesRequired = ValueSerializer.bytesRequired(value)
    val bytes = Slice.create[Byte](bytesRequired)
    serializer.write(value, bytes)
    bytes
  }

  def write[T](value: T)(bytes: Slice[Byte])(implicit serializer: ValueSerializer[T]): Unit =
    serializer.write(value, bytes)

  def read[T](value: Slice[Byte])(implicit serializer: ValueSerializer[T]): T =
    serializer.read(value)

  def read[T](reader: ReaderBase)(implicit serializer: ValueSerializer[T]): T =
    serializer.read(reader)

  def bytesRequired[T](value: T)(implicit serializer: ValueSerializer[T]): Int =
    serializer.bytesRequired(value)
}
