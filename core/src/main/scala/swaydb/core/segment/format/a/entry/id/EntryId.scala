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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.id

//TODO - change id to be Slice[Byte] with custom ordering to support better backward compatibility.
abstract class EntryId(val id: Int)
object EntryId {

  trait EntryFormat {
    //@formatter:off
    def
    keyIdsList: List[EntryId]
    def format: EntryId.Format
    //@formatter:on
  }

  trait Format {
    //@formatter:off
    def keyFullyCompressed: Key.FullyCompressed
    def keyPartiallyCompressed: Key.PartiallyCompressed
    def keyUncompressed: Key.Uncompressed
    //@formatter:on
  }
  object Format {
    trait A extends Format
  }

  trait Key {
    //@formatter:off
    def noTime: Time.NoTime
    def timeFullyCompressed: Time.FullyCompressed
    def timePartiallyCompressed: Time.PartiallyCompressed
    def timeUncompressed: Time.Uncompressed
    //@formatter:on
  }

  object Key {
    trait FullyCompressed extends Key
    trait PartiallyCompressed extends Key
    trait Uncompressed extends Key
  }

  trait Time {
    //@formatter:off
    def noValue: Value.NoValue
    def valueFullyCompressed: Value.FullyCompressed
    def valueUncompressed: Value.Uncompressed
    //@formatter:on
  }
  object Time {
    trait NoTime extends Time
    trait PartiallyCompressed extends Time
    trait Uncompressed extends Time
    trait FullyCompressed extends Time
  }

  sealed trait GetDeadlineId {
    //@formatter:off
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
    //@formatter:on
  }

  trait Value
  object Value {
    trait NoValue extends Value with GetDeadlineId
    trait FullyCompressed extends Value with GetDeadlineId
    trait Uncompressed extends Value {
      //@formatter:off
      def valueOffsetOneCompressed: ValueOffset.OneCompressed
      def valueOffsetTwoCompressed: ValueOffset.TwoCompressed
      def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed
      def valueOffsetUncompressed: ValueOffset.Uncompressed
      //@formatter:on
    }
  }

  trait ValueOffset {
    //@formatter:off
    def valueLengthOneCompressed: ValueLength.OneCompressed
    def valueLengthTwoCompressed: ValueLength.TwoCompressed
    def valueLengthThreeCompressed: ValueLength.ThreeCompressed
    def valueLengthFullyCompressed: ValueLength.FullyCompressed
    def valueLengthUncompressed: ValueLength.Uncompressed
    //@formatter:on
  }
  object ValueOffset {
    trait OneCompressed extends ValueOffset
    trait TwoCompressed extends ValueOffset
    trait ThreeCompressed extends ValueOffset
    trait Uncompressed extends ValueOffset
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

    def minId: Int

    def maxId: Int

    def hasId(id: Int): Boolean =
      id >= minId && id <= maxId

    def adjustToBaseId(id: Int): Int =
      id - minId

    def adjustToEntryId(id: Int): Int =
      id + minId
  }

  object Put extends Id {
    val minId = 0
    val maxId = 3000
  }

  object Group extends Id {
    val minId = Put.maxId + 1
    val maxId = Put.maxId * 2
  }

  /**
    * Reserve 1 & 2 bytes ids for Put and Group. All the following key-values
    * disappear in last Level but [[Put]] and [[Group]] are kept unless deleted.
    */
  object Range extends Id {
    val minId = 16384
    val maxId = minId + 3000
  }

  object Remove extends Id {
    val minId = Range.maxId + 1
    val maxId = Range.maxId * 2
  }

  object Update extends Id {
    val minId = Remove.maxId + 1
    val maxId = Remove.maxId * 2
  }

  object Function extends Id {
    val minId = Update.maxId + 1
    val maxId = Update.maxId * 2
  }

  object PendingApply extends Id {
    val minId = Function.maxId + 1
    val maxId = Function.maxId * 2
  }
}
