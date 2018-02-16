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

import swaydb.core.data.KeyValueReadOnly
import swaydb.core.segment.format.one.MatchResult.{Matched, _}
import swaydb.data.slice.Slice

sealed trait MatchResult[+K]

object MatchResult {
  case class Matched[K <: KeyValueReadOnly](result: K) extends MatchResult[K]
  case object Next extends MatchResult[Nothing]
  case object Stop extends MatchResult[Nothing]
}

private[core] sealed trait KeyMatcher {
  val key: Slice[Byte]

  def apply[K <: KeyValueReadOnly](previous: K,
                                   next: Option[K],
                                   hasMore: => Boolean): MatchResult[K]
}

private[core] object KeyMatcher {
  case class Exact(key: Slice[Byte])(implicit ordering: Ordering[Slice[Byte]]) extends KeyMatcher {

    override def apply[K <: KeyValueReadOnly](previous: K,
                                              next: Option[K],
                                              hasMore: => Boolean): MatchResult[K] =
      next match {
        case Some(next) =>
          val nextMatch = ordering.compare(next.key, key)
          if (nextMatch == 0)
            Matched(next)
          else if (nextMatch < 0 && hasMore)
            Next
          //next key-value should never be read if previous was a match. This should not occur in actual scenarios.
          //          else if (nextMatch > 0 && ordering.compare(previous.key, key) == 0)
          //            Matched(previous)
          else
            Stop

        case None =>
          val previousMatch = ordering.compare(previous.key, key)

          if (previousMatch == 0)
            Matched(previous)

          else if (previousMatch < 0 && hasMore)
            Next
          else
            Stop
      }
  }

  case class Lower(key: Slice[Byte])(implicit ordering: Ordering[Slice[Byte]]) extends KeyMatcher {

    override def apply[K <: KeyValueReadOnly](previous: K,
                                              next: Option[K],
                                              hasMore: => Boolean): MatchResult[K] = {
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
              Next
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
              Next
            else
              Matched(previous)
          else
            Stop
      }
    }
  }

  case class Higher(key: Slice[Byte])(implicit ordering: Ordering[Slice[Byte]]) extends KeyMatcher {

    override def apply[K <: KeyValueReadOnly](previous: K,
                                              next: Option[K],
                                              hasMore: => Boolean): MatchResult[K] = {
      next match {
        case Some(next) =>
          val nextCompare = ordering.compare(next.key, key)
          if (nextCompare > 0)
            Matched(next)
          else if (nextCompare <= 0 && hasMore)
            Next
          else
            Stop

        case None =>
          val previousCompare = ordering.compare(previous.key, key)
          if (previousCompare > 0)
            Matched(previous)
          else if (previousCompare <= 0 && hasMore)
            Next
          else
            Stop
      }
    }
  }

}