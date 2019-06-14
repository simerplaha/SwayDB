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

package swaydb.core.segment.format.a.entry.id

import swaydb.core.segment.format.a.entry.reader.EntryReader

//TODO - change id to be Slice[Byte] with custom ordering to support better backward compatibility.
abstract class EntryId(val id: Int)
object EntryId {

  trait EntryFormat {
    def keyIdsList: List[EntryId]
    def format: EntryId.Format
  }

  trait Format {
    def start: Key
  }
  object Format {
    trait A extends Format
  }

  trait Key {
    def noTime: Time.NoTime
    def timePartiallyCompressed: Time.PartiallyCompressed
    def timeUncompressed: Time.Uncompressed
  }

  trait Time {
    def noValue: Value.NoValue
    def valueFullyCompressed: Value.FullyCompressed
    def valueUncompressed: Value.Uncompressed
  }
  object Time {
    trait PartiallyCompressed extends Time
    trait Uncompressed extends Time
    trait NoTime extends Time
  }

  sealed trait GetDeadlineId {
    def deadlineOneCompressed: Deadline.OneCompressed
    def deadlineTwoCompressed: Deadline.TwoCompressed
    def deadlineThreeCompressed: Deadline.ThreeCompressed
    def deadlineFourCompressed: Deadline.FourCompressed
    def deadlineFiveCompressed: Deadline.FiveCompressed
    def deadlineSixCompressed: Deadline.SixCompressed
    def deadlineSevenCompressed: Deadline.SevenCompressed
    def deadlineFullyCompressed: Deadline.FullyCompressed
    def deadlineUncompressed: Deadline.Uncompressed
    def noDeadline: Deadline.NoDeadline
  }

  trait Value
  object Value {
    trait NoValue extends Value with GetDeadlineId
    trait FullyCompressed extends Value {
      def valueOffsetOneCompressed: ValueOffset.OneCompressed
      def valueOffsetTwoCompressed: ValueOffset.TwoCompressed
      def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed
      def valueOffsetFullyCompressed: ValueOffset.FullyCompressed
      def valueOffsetUncompressed: ValueOffset.Uncompressed
    }
    trait Uncompressed extends Value {
      def valueOffsetOneCompressed: ValueOffset.OneCompressed
      def valueOffsetTwoCompressed: ValueOffset.TwoCompressed
      def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed
      def valueOffsetUncompressed: ValueOffset.Uncompressed
    }
  }

  trait ValueOffset {
    def valueLengthOneCompressed: ValueLength.OneCompressed
    def valueLengthTwoCompressed: ValueLength.TwoCompressed
    def valueLengthThreeCompressed: ValueLength.ThreeCompressed
    def valueLengthFullyCompressed: ValueLength.FullyCompressed
    def valueLengthUncompressed: ValueLength.Uncompressed
  }
  object ValueOffset {
    trait OneCompressed extends ValueOffset
    trait TwoCompressed extends ValueOffset
    trait ThreeCompressed extends ValueOffset
    trait Uncompressed extends ValueOffset
    trait FullyCompressed extends ValueOffset
  }

  trait ValueLength extends GetDeadlineId
  object ValueLength {
    trait OneCompressed extends ValueLength
    trait TwoCompressed extends ValueLength
    trait ThreeCompressed extends ValueLength
    trait FullyCompressed extends ValueLength
    trait Uncompressed extends ValueLength
  }

  trait Deadline {
    def id: Int
  }
  object Deadline {
    trait NoDeadline extends Deadline
    trait OneCompressed extends Deadline
    trait TwoCompressed extends Deadline
    trait ThreeCompressed extends Deadline
    trait FourCompressed extends Deadline
    trait FiveCompressed extends Deadline
    trait SixCompressed extends Deadline
    trait SevenCompressed extends Deadline
    trait FullyCompressed extends Deadline
    trait Uncompressed extends Deadline
  }

  sealed trait Id {

    def minKeyPartiallyCompressedId: Int
    def maxKeyPartiallyCompressedId: Int

    def minKeyUncompressedId: Int
    def maxKeyUncompressedId: Int

    def hasId(id: Int): Boolean =
      id >= minKeyPartiallyCompressedId && id <= maxKeyUncompressedId

    def isPartiallyCompressedKey(id: Int): Boolean =
      id >= minKeyPartiallyCompressedId && id <= maxKeyPartiallyCompressedId

    def isUncompressedKey(id: Int): Boolean =
      id >= minKeyUncompressedId && id <= maxKeyUncompressedId

    /**
      * Given persisted entryID convert it to
      */
    def adjustEntryIdToBaseId(id: Int): Int =
      if (isPartiallyCompressedKey(id))
        if (minKeyPartiallyCompressedId == Put.minKeyPartiallyCompressedId)
          id
        else
          id - minKeyPartiallyCompressedId
      else if (isUncompressedKey(id))
        id - minKeyUncompressedId
      else
      //this exception is not expected to occur. This may only occur due to file corruption.
      //instead of wrapping in IO for performance throw exception as this is not expected to occur.
      //if it does then it will be caught higher up in SegmentReader before responding the user.
        throw new Exception(s"Invalid ${this.getClass.getSimpleName} id: $id")

    def adjustBaseToEntryId(id: Int): Int =
      if (isPartiallyCompressedKey(id))
        if (minKeyPartiallyCompressedId == Put.minKeyPartiallyCompressedId) //if it's put the ids are the same as base entry.
          id
        else
          id + minKeyPartiallyCompressedId
      else if (isUncompressedKey(id))
        id + minKeyUncompressedId
      else
      //this exception is not expected to occur. This may only occur due to file corruption.
      //instead of wrapping in IO for performance throw exception as this is not expected to occur.
      //if it does then it will be caught higher up in SegmentReader before responding the user.
        throw new Exception(s"Invalid ${this.getClass.getSimpleName} id: $id")

    def adjustToKeyUncompressed(id: Int): Int =
      id + minKeyUncompressedId

    def adjustBaseToEntryIdAndKeyUncompressed(id: Int): Int =
      adjustToKeyUncompressed(adjustBaseToEntryId(id))
  }

  //Last max id used in BaseEntryId.
  val reservedKeysPerGroup = EntryReader.readers.last.maxID

  object Put extends Id {
    override val minKeyPartiallyCompressedId: Int = EntryReader.readers.head.minID
    override val maxKeyPartiallyCompressedId: Int = reservedKeysPerGroup
    override val minKeyUncompressedId: Int = reservedKeysPerGroup + 1
    override val maxKeyUncompressedId: Int = minKeyUncompressedId + reservedKeysPerGroup
  }

  object Group extends Id {
    override val minKeyPartiallyCompressedId: Int = Put.maxKeyUncompressedId + 1
    override val maxKeyPartiallyCompressedId: Int = minKeyPartiallyCompressedId + reservedKeysPerGroup
    override val minKeyUncompressedId: Int = maxKeyPartiallyCompressedId + 1
    override val maxKeyUncompressedId: Int = minKeyUncompressedId + reservedKeysPerGroup
  }

  /**
    * Reserve 1 & 2 bytes ids for Put and Group. All the following key-values
    * disappear in last Level but [[Put]] and [[Group]] are kept unless deleted.
    */
  object Range extends Id {
    override val minKeyPartiallyCompressedId: Int = 16384
    override val maxKeyPartiallyCompressedId: Int = minKeyPartiallyCompressedId + reservedKeysPerGroup
    override val minKeyUncompressedId: Int = maxKeyPartiallyCompressedId + 1
    override val maxKeyUncompressedId: Int = minKeyUncompressedId + reservedKeysPerGroup
  }

  object Remove extends Id {

    override val minKeyPartiallyCompressedId: Int = Range.maxKeyUncompressedId + 1
    override val maxKeyPartiallyCompressedId: Int = minKeyPartiallyCompressedId + reservedKeysPerGroup
    override val minKeyUncompressedId: Int = maxKeyPartiallyCompressedId + 1
    override val maxKeyUncompressedId: Int = minKeyUncompressedId + reservedKeysPerGroup
  }

  object Update extends Id {
    override val minKeyPartiallyCompressedId: Int = Remove.maxKeyUncompressedId + 1
    override val maxKeyPartiallyCompressedId: Int = minKeyPartiallyCompressedId + reservedKeysPerGroup
    override val minKeyUncompressedId: Int = maxKeyPartiallyCompressedId + 1
    override val maxKeyUncompressedId: Int = minKeyUncompressedId + reservedKeysPerGroup
  }

  object Function extends Id {
    override val minKeyPartiallyCompressedId: Int = Update.maxKeyUncompressedId + 1
    override val maxKeyPartiallyCompressedId: Int = minKeyPartiallyCompressedId + reservedKeysPerGroup
    override val minKeyUncompressedId: Int = maxKeyPartiallyCompressedId + 1
    override val maxKeyUncompressedId: Int = minKeyUncompressedId + reservedKeysPerGroup
  }

  object PendingApply extends Id {
    override val minKeyPartiallyCompressedId: Int = Function.maxKeyUncompressedId + 1
    override val maxKeyPartiallyCompressedId: Int = minKeyPartiallyCompressedId + reservedKeysPerGroup
    override val minKeyUncompressedId: Int = maxKeyPartiallyCompressedId + 1
    override val maxKeyUncompressedId: Int = minKeyUncompressedId + reservedKeysPerGroup
  }
}
