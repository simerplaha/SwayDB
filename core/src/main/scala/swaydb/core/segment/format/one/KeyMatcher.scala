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

package swaydb.core.segment.format.one

import swaydb.core.data.SegmentEntry.RangeReadOnly
import swaydb.core.data.SegmentEntryReadOnly
import swaydb.core.segment.format.one.MatchResult.{Matched, _}
import swaydb.data.slice.Slice

sealed trait MatchResult

object MatchResult {
  case class Matched(result: SegmentEntryReadOnly) extends MatchResult
  case object Next extends MatchResult
  case object Stop extends MatchResult
}

private[core] sealed trait KeyMatcher {
  val key: Slice[Byte]

  def apply(previous: SegmentEntryReadOnly,
            next: Option[SegmentEntryReadOnly],
            hasMore: => Boolean): MatchResult
}

private[core] object KeyMatcher {
  case class Get(key: Slice[Byte])(implicit ordering: Ordering[Slice[Byte]]) extends KeyMatcher {

    override def apply(previous: SegmentEntryReadOnly,
                       next: Option[SegmentEntryReadOnly],
                       hasMore: => Boolean): MatchResult =
      next.getOrElse(previous) match {
        case range: RangeReadOnly =>
          val fromKeyMatch = ordering.compare(key, range.fromKey)
          val toKeyMatch = ordering.compare(key, range.toKey)
          if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
            Matched(range)
          else if (fromKeyMatch > 0 && hasMore)
            Next
          else
            Stop

        case keyValue =>
          val matchResult = ordering.compare(key, keyValue.key)
          if (matchResult == 0)
            Matched(keyValue)
          else if (matchResult > 0 && hasMore)
            Next
          else
            Stop
      }
  }

  case class Lower(key: Slice[Byte])(implicit ordering: Ordering[Slice[Byte]]) extends KeyMatcher {

    override def apply(previous: SegmentEntryReadOnly,
                       next: Option[SegmentEntryReadOnly],
                       hasMore: => Boolean): MatchResult = {
      next match {
        case Some(next) =>
          val nextCompare = ordering.compare(next.key, key)
          if (nextCompare >= 0) {
            val previousCompare = ordering.compare(previous.key, key)
            if (previousCompare < 0)
              Matched(previous)
            else
              Stop
          }
          else if (nextCompare < 0)
            if (hasMore)
              next match {
                case range: RangeReadOnly if ordering.compare(key, range.toKey) <= 0 =>
                  Matched(next)

                case _ =>
                  Next
              }
            else
              Matched(next)
          else
            Stop

        case None =>
          val previousCompare = ordering.compare(previous.key, key)
          if (previousCompare == 0)
            Stop
          else if (previousCompare < 0)
            if (hasMore)
              previous match {
                case range: RangeReadOnly if ordering.compare(key, range.toKey) <= 0 =>
                  Matched(previous)

                case _ =>
                  Next
              }
            else
              Matched(previous)
          else
            Stop
      }
    }
  }

  case class Higher(key: Slice[Byte])(implicit ordering: Ordering[Slice[Byte]]) extends KeyMatcher {

    override def apply(previous: SegmentEntryReadOnly,
                       next: Option[SegmentEntryReadOnly],
                       hasMore: => Boolean): MatchResult = {
      val keyValue = next getOrElse previous
      val nextCompare = ordering.compare(keyValue.key, key)
      if (nextCompare > 0)
        Matched(keyValue)
      else if (nextCompare <= 0)
        keyValue match {
          case range: RangeReadOnly if ordering.compare(key, range.toKey) < 0 =>
            Matched(keyValue)

          case _ =>
            if (hasMore)
              Next
            else
              Stop
        }
      else
        Stop
    }
  }

}