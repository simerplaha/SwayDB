/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.entry.id

private[segment] abstract class BaseEntryId(val baseId: Int)
private[segment] object BaseEntryId {

  trait BaseEntryIdFormat {
    def baseIds: List[BaseEntryId]
    def format: BaseEntryId.Format
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

  sealed trait DeadlineId {
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
    trait NoValue extends Value with DeadlineId
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

  trait ValueLength extends DeadlineId
  object ValueLength {
    trait OneCompressed extends ValueLength
    trait TwoCompressed extends ValueLength
    trait ThreeCompressed extends ValueLength
    trait FullyCompressed extends ValueLength
    trait Uncompressed extends ValueLength
  }

  trait Deadline {
    def baseId: Int
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
}
