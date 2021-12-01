/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.serialiser

import swaydb.core.file.reader.Reader
import swaydb.core.segment.data.{Time, Value}
import swaydb.core.util.Times._
import swaydb.core.util.{Bytes, MinMax}
import swaydb.slice.{ReaderBase, Slice, SliceMut, SliceOption}
import swaydb.utils.ByteSizeOf
import swaydb.utils.Options.OptionsImplicits

import scala.annotation.implicitNotFound
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline

@implicitNotFound("Type class implementation not found for ValueSerialiser of type ${T}")
private[core] sealed trait ValueSerialiser[T] {

  def write(value: T, bytes: SliceMut[Byte]): Unit

  def read(reader: ReaderBase): T

  def read(bytes: Slice[Byte]): T =
    read(Reader(bytes))

  def bytesRequired(value: T): Int
}

private[core] object ValueSerialiser {

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

  def readValue(reader: ReaderBase): SliceOption[Byte] = {
    val remaining = reader.readRemaining()
    if (remaining.isEmpty)
      Slice.Null
    else
      remaining
  }

  implicit object ValuePutSerialiser extends ValueSerialiser[Value.Put] {

    override def write(value: Value.Put, bytes: SliceMut[Byte]): Unit =
      bytes
        .addUnsignedLong(value.deadline.toNanos)
        .addUnsignedInt(value.time.size)
        .addAll(value.time.time)
        .addAll(value.value.getOrElseC(Slice.emptyBytes))

    override def bytesRequired(value: Value.Put): Int =
      Bytes.sizeOfUnsignedLong(value.deadline.toNanos) +
        Bytes.sizeOfUnsignedInt(value.time.size) +
        value.time.size +
        value.value.valueOrElseC(_.size, 0)

    override def read(reader: ReaderBase): Value.Put = {
      val deadline = readDeadline(reader)
      val time = readTime(reader)
      val value = readValue(reader)
      Value.Put(value, deadline, time)
    }
  }

  implicit object ValueUpdateSerialiser extends ValueSerialiser[Value.Update] {

    override def write(value: Value.Update, bytes: SliceMut[Byte]): Unit =
      bytes
        .addUnsignedLong(value.deadline.toNanos)
        .addUnsignedInt(value.time.size)
        .addAll(value.time.time)
        .addAll(value.value.getOrElseC(Slice.emptyBytes))

    override def bytesRequired(value: Value.Update): Int =
      Bytes.sizeOfUnsignedLong(value.deadline.toNanos) +
        Bytes.sizeOfUnsignedInt(value.time.size) +
        value.time.size +
        value.value.valueOrElseC(_.size, 0)

    override def read(reader: ReaderBase): Value.Update = {
      val deadline = readDeadline(reader)
      val time = readTime(reader)
      val value = readValue(reader)
      Value.Update(value, deadline, time)
    }
  }

  implicit object ValueRemoveSerialiser extends ValueSerialiser[Value.Remove] {

    override def write(value: Value.Remove, bytes: SliceMut[Byte]): Unit =
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

  implicit object ValueFunctionSerialiser extends ValueSerialiser[Value.Function] {
    override def write(value: Value.Function, bytes: SliceMut[Byte]): Unit =
      ValueSerialiser.write((value.function, value.time.time))(bytes)(TupleOfBytesSerialiser)

    override def bytesRequired(value: Value.Function): Int =
      ValueSerialiser.bytesRequired((value.function, value.time.time))(TupleOfBytesSerialiser)

    override def read(reader: ReaderBase): Value.Function = {
      val (function, time) = ValueSerialiser.read[(Slice[Byte], Slice[Byte])](reader)
      Value.Function(function, Time(time))
    }
  }

  implicit object ValueSliceApplySerialiser extends ValueSerialiser[Slice[Value.Apply]] {

    override def write(applies: Slice[Value.Apply], bytes: SliceMut[Byte]): Unit = {
      bytes.addUnsignedInt(applies.size)
      applies foreach {
        case value: Value.Update =>
          val bytesRequired = ValueSerialiser.bytesRequired(value)
          ValueSerialiser.write(value)(bytes.addUnsignedInt(0).addUnsignedInt(bytesRequired))

        case value: Value.Function =>
          val bytesRequired = ValueSerialiser.bytesRequired(value)
          ValueSerialiser.write(value)(bytes.addUnsignedInt(1).addUnsignedInt(bytesRequired))

        case value: Value.Remove =>
          val bytesRequired = ValueSerialiser.bytesRequired(value)
          ValueSerialiser.write(value)(bytes.addUnsignedInt(2).addUnsignedInt(bytesRequired))
      }
    }

    override def bytesRequired(value: Slice[Value.Apply]): Int =
    //also add the total number of entries.
      value.foldLeft(Bytes.sizeOfUnsignedInt(value.size)) {
        case (total, function) =>
          function match {
            case value: Value.Update =>
              val bytesRequired = ValueSerialiser.bytesRequired(value)
              total + Bytes.sizeOfUnsignedInt(0) + Bytes.sizeOfUnsignedInt(bytesRequired) + bytesRequired

            case value: Value.Function =>
              val bytesRequired = ValueSerialiser.bytesRequired(value)
              total + Bytes.sizeOfUnsignedInt(1) + Bytes.sizeOfUnsignedInt(bytesRequired) + bytesRequired

            case value: Value.Remove =>
              val bytesRequired = ValueSerialiser.bytesRequired(value)
              total + Bytes.sizeOfUnsignedInt(2) + Bytes.sizeOfUnsignedInt(bytesRequired) + bytesRequired
          }
      }

    override def read(reader: ReaderBase): Slice[Value.Apply] = {
      val count = reader.readUnsignedInt()
      reader.foldLeft(Slice.of[Value.Apply](count)) {
        case (applies, reader) =>
          val id = reader.readUnsignedInt()
          val bytes = reader.readUnsignedIntSized()
          if (id == 0) {
            val update = ValueSerialiser.read[Value.Update](Reader(bytes))
            applies add update
            applies
          } else if (id == 1) {
            val update = ValueSerialiser.read[Value.Function](Reader(bytes))
            applies add update
            applies
          } else if (id == 2) {
            val update = ValueSerialiser.read[Value.Remove](Reader(bytes))
            applies add update
            applies
          }
          else
            throw new Exception(s"Invalid id:$id")
      }
    }
  }

  implicit object ValuePendingApplySerialiser extends ValueSerialiser[Value.PendingApply] {

    override def write(value: Value.PendingApply, bytes: SliceMut[Byte]): Unit =
      ValueSerialiser.write(value.applies)(bytes)

    override def bytesRequired(value: Value.PendingApply): Int =
      ValueSerialiser.bytesRequired(value.applies)

    override def read(reader: ReaderBase): Value.PendingApply =
      Value.PendingApply(ValueSerialiser.read[Slice[Value.Apply]](reader))
  }

  /**
   * Serialiser for a tuple of Option bytes and sequence bytes.
   */
  implicit object SeqOfBytesSerialiser extends ValueSerialiser[Iterable[Slice[Byte]]] {

    override def write(values: Iterable[Slice[Byte]], bytes: SliceMut[Byte]): Unit =
      values foreach {
        value =>
          bytes
            .addUnsignedInt(value.size)
            .addAll(value)
      }

    override def bytesRequired(values: Iterable[Slice[Byte]]): Int =
      values.foldLeft(0) {
        case (size, valueBytes) =>
          size + Bytes.sizeOfUnsignedInt(valueBytes.size) + valueBytes.size
      }

    override def read(reader: ReaderBase): Iterable[Slice[Byte]] =
      reader.foldLeft(ListBuffer.empty[Slice[Byte]]) {
        case (result, reader) =>
          val size = reader.readUnsignedInt()
          val bytes = reader.read(size)
          result += bytes
      }
  }

  /**
   * Serialiser for a tuple of Option bytes and sequence bytes.
   */
  implicit object TupleOfBytesSerialiser extends ValueSerialiser[(Slice[Byte], Slice[Byte])] {

    override def write(value: (Slice[Byte], Slice[Byte]), bytes: SliceMut[Byte]): Unit =
      SeqOfBytesSerialiser.write(Seq(value._1, value._2), bytes)

    override def bytesRequired(value: (Slice[Byte], Slice[Byte])): Int =
      SeqOfBytesSerialiser.bytesRequired(Seq(value._1, value._2))

    override def read(reader: ReaderBase): (Slice[Byte], Slice[Byte]) = {
      val bytes = SeqOfBytesSerialiser.read(reader)
      if (bytes.size != 2)
        throw new Exception(TupleOfBytesSerialiser.getClass.getSimpleName + s".read did not return a tuple. Size = ${bytes.size}")
      else
        (bytes.head, bytes.last)
    }
  }

  /**
   * Serialiser for a tuple of Option bytes and sequence bytes.
   */
  implicit object TupleBytesAndOptionBytesSerialiser extends ValueSerialiser[(Slice[Byte], SliceOption[Byte])] {

    override def write(value: (Slice[Byte], SliceOption[Byte]), bytes: SliceMut[Byte]): Unit =
      value._2 match {
        case second: Slice[Byte] =>
          bytes.addSignedInt(1)
          ValueSerialiser.write[(Slice[Byte], Slice[Byte])]((value._1, second))(bytes)

        case Slice.Null =>
          bytes.addSignedInt(0)
          bytes.addAll(value._1)
      }

    override def bytesRequired(value: (Slice[Byte], SliceOption[Byte])): Int =
      value._2 match {
        case second: Slice[Byte] =>
          1 +
            ValueSerialiser.bytesRequired[(Slice[Byte], Slice[Byte])]((value._1, second))

        case Slice.Null =>
          1 +
            value._1.size
      }

    override def read(reader: ReaderBase): (Slice[Byte], SliceOption[Byte]) = {
      val id = reader.readUnsignedInt()
      if (id == 0) {
        val all = reader.readRemaining()
        (all, Slice.Null)
      }
      else {
        val (left, right) = ValueSerialiser.read[(Slice[Byte], Slice[Byte])](reader)
        (left, right)
      }
    }
  }

  /**
   * Serialiser for a tuple of Option bytes and sequence bytes.
   */
  implicit object IntMapListBufferSerialiser extends ValueSerialiser[mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]]] {
    val formatId = 0.toByte

    override def write(map: mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]], bytes: SliceMut[Byte]): Unit = {
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
        throw new Exception(s"Invalid formatID: $format")
      else
        reader.foldLeft(mutable.Map.empty[Int, Iterable[(Slice[Byte], Slice[Byte])]]) {
          case (map, reader) =>
            val int = reader.readUnsignedInt()
            val tuplesCount = reader.readUnsignedInt()

            val tuples = Slice.of[(Slice[Byte], Slice[Byte])](tuplesCount)

            var i = 0
            while (i < tuplesCount) {
              val leftSize = reader.readUnsignedInt()
              val left = reader.read(leftSize)
              val rightSize = reader.readUnsignedInt()
              val right = reader.read(rightSize)
              tuples.add((left, right))
              i += 1
            }

            map.put(int, tuples)
            map
        }
    }

    /**
     * Calculates the number of bytes required with minimal information about the RangeFilter.
     */
    def optimalBytesRequired(rangeCount: Int,
                             maxUncommonBytesToStore: Int,
                             rangeFilterCommonPrefixes: Iterable[Int]): Int =
      ByteSizeOf.byte + //formatId
        rangeFilterCommonPrefixes.foldLeft(0)(_ + Bytes.sizeOfUnsignedInt(_)) + //common prefix bytes sizes
        //Bytes.sizeOf(numberOfRanges) because there can only be a max of numberOfRanges per group so ByteSizeOf.int is not required.
        (Bytes.sizeOfUnsignedInt(rangeCount) * rangeFilterCommonPrefixes.size) + //tuples count per common prefix count
        (rangeCount * Bytes.sizeOfUnsignedInt(maxUncommonBytesToStore) * 2) +
        (rangeCount * maxUncommonBytesToStore * 2) //store the bytes itself, * 2 because it's a tuple.

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

  implicit object MinMaxSerialiser extends ValueSerialiser[Option[MinMax[Slice[Byte]]]] {
    override def write(minMax: Option[MinMax[Slice[Byte]]], bytes: SliceMut[Byte]): Unit =
      minMax match {
        case Some(minMax) =>
          bytes addUnsignedInt minMax.min.size
          bytes addAll minMax.min
          minMax.max match {
            case Some(max) =>
              bytes addUnsignedInt max.size
              bytes addAll max

            case None =>
              bytes addUnsignedInt 0
          }

        case None =>
          bytes addUnsignedInt 0
      }

    override def read(reader: ReaderBase): Option[MinMax[Slice[Byte]]] = {
      val minIdSize = reader.readUnsignedInt()
      if (minIdSize == 0)
        None
      else {
        val minId = reader.read(minIdSize)
        val maxIdSize = reader.readUnsignedInt()
        val maxId = if (maxIdSize == 0) None else Some(reader.read(maxIdSize))
        Some(MinMax(minId, maxId))
      }
    }

    override def bytesRequired(minMax: Option[MinMax[Slice[Byte]]]): Int =
      minMax match {
        case Some(minMax) =>
          Bytes.sizeOfUnsignedInt(minMax.min.size) +
            minMax.min.size +
            Bytes.sizeOfUnsignedInt(minMax.max.valueOrElse(_.size, 0)) +
            minMax.max.valueOrElse(_.size, 0)

        case None =>
          1
      }
  }

  def writeBytes[T](value: T)(implicit serialiser: ValueSerialiser[T]): Slice[Byte] = {
    val bytesRequired = ValueSerialiser.bytesRequired(value)
    val bytes = Slice.of[Byte](bytesRequired)
    serialiser.write(value, bytes)
    bytes
  }

  def write[T](value: T)(bytes: SliceMut[Byte])(implicit serialiser: ValueSerialiser[T]): Unit =
    serialiser.write(value, bytes)

  def read[T](value: Slice[Byte])(implicit serialiser: ValueSerialiser[T]): T =
    serialiser.read(value)

  def read[T](reader: ReaderBase)(implicit serialiser: ValueSerialiser[T]): T =
    serialiser.read(reader)

  def bytesRequired[T](value: T)(implicit serialiser: ValueSerialiser[T]): Int =
    serialiser.bytesRequired(value)
}
