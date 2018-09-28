/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.format.one.entry.id

abstract class EntryId(val id: Int)
object EntryId {

  //  val keyIdsList: List[EntryId] = SealedList.list[EntryId]

  //  private def ids = keyIdsList.map(_.id)
  //
  //  //ensure that there are duplicate keys.
  //  assert(ids.distinct.size == ids.size, s"EntryId contain duplicate ids '${ids.diff(ids.distinct).mkString(", ")}'")
  //  //assert that all id integers are used.
  //  ids.sorted.foldLeft(-1) {
  //    case (previous, next) =>
  //      assert(next - previous == 1, s"Missing ID: previous: $previous -> next: $next")
  //      next
  //  }
  //
  //  private val keyIdsMap: Map[Int, EntryId] =
  //    keyIdsList map {
  //      keyId =>
  //        keyId.id -> keyId
  //    } toMap

  trait Entry {
    //@formatter:off
    def keyFullyCompressed: Key.FullyCompressed
    def keyPartiallyCompressed: Key.PartiallyCompressed
    def keyUncompressed: Key.Uncompressed
    //@formatter:on
  }
  object Entry {
    trait Remove extends Entry
    trait Put extends Entry
    trait Update extends Entry
  }

  trait Key {
    //@formatter:off
    def noValue: Value.NoValue
    def valueFullyCompressed: Value.FullyCompressed
    def valueUncompressed: Value.Uncompressed
    //@formatter:on
  }
  object Key {
    trait FullyCompressed extends Key
    trait PartiallyCompressed extends Key
    trait Uncompressed extends Key
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
}