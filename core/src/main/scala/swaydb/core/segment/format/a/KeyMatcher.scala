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

package swaydb.core.segment.format.a

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.MatchResult.{Matched, _}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

sealed trait MatchResult

object MatchResult {
  case class Matched(result: Persistent) extends MatchResult
  case object Next extends MatchResult
  case object Stop extends MatchResult
}

private[core] sealed trait KeyMatcher {
  val key: Slice[Byte]

  def apply(previous: Persistent,
            next: Option[Persistent],
            hasMore: => Boolean): MatchResult
}

private[core] object KeyMatcher {
  case class Get(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]) extends KeyMatcher {

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult =
      next.getOrElse(previous) match {
        case fixed: Persistent.Fixed =>
          val matchResult = keyOrder.compare(key, fixed.key)
          if (matchResult == 0)
            Matched(fixed)
          else if (matchResult > 0 && hasMore)
            Next
          else
            Stop

        case group: Persistent.Group =>
          val fromKeyMatch = keyOrder.compare(key, group.minKey)
          val toKeyMatch: Int = keyOrder.compare(key, group.maxKey.maxKey)
          if (fromKeyMatch >= 0 && ((group.maxKey.inclusive && toKeyMatch <= 0) || (!group.maxKey.inclusive && toKeyMatch < 0))) //is within the range
            Matched(group)
          else if (toKeyMatch >= 0 && hasMore)
            Next
          else
            Stop

        case range: Persistent.Range =>
          val fromKeyMatch = keyOrder.compare(key, range.fromKey)
          val toKeyMatch = keyOrder.compare(key, range.toKey)
          if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
            Matched(range)
          else if (toKeyMatch >= 0 && hasMore)
            Next
          else
            Stop
      }
  }

  case class Lower(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]) extends KeyMatcher {

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult =
      next match {
        case Some(next) =>
          val nextCompare = keyOrder.compare(next.key, key)
          if (nextCompare >= 0) {
            val previousCompare = keyOrder.compare(previous.key, key)
            if (previousCompare < 0)
              Matched(previous)
            else
              Stop
          } else if (nextCompare < 0) {
            if (hasMore)
              next match {
                case range: Persistent.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(next)

                case group: Persistent.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
                  Matched(next)

                case _ =>
                  Next
              }
            else
              Matched(next)
          } else {
            Stop
          }

        case None =>
          val previousCompare = keyOrder.compare(previous.key, key)
          if (previousCompare == 0)
            Stop
          else if (previousCompare < 0)
            if (hasMore)
              previous match {
                case range: Persistent.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(previous)

                case group: Persistent.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
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

  case class Higher(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]) extends KeyMatcher {

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult = {
      val keyValue = next getOrElse previous
      val nextCompare = keyOrder.compare(keyValue.key, key)
      if (nextCompare > 0)
        Matched(keyValue)
      else if (nextCompare <= 0)
        keyValue match {
          case range: Persistent.Range if keyOrder.compare(key, range.toKey) < 0 =>
            Matched(keyValue)

          case group: Persistent.Group if keyOrder.compare(key, group.maxKey.maxKey) < 0 =>
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
