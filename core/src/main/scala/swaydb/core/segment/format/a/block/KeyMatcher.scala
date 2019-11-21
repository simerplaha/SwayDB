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

package swaydb.core.segment.format.a.block

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.KeyMatcher.Result.{AheadOrNoneOrEnd, BehindFetchNext, BehindStopped, Matched}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] sealed trait KeyMatcher {
  def key: Slice[Byte]

  def apply(previous: Persistent.Partial,
            next: Option[Persistent.Partial],
            hasMore: => Boolean): KeyMatcher.Result

  def keyOrder: KeyOrder[Slice[Byte]]

  def matchOnly: Boolean
}

private[core] object KeyMatcher {

  sealed trait Result
  object Result {

    sealed trait Complete extends Result
    sealed trait InComplete extends Result

    class Matched(val previous: Option[Persistent.Partial],
                  val result: Persistent.Partial,
                  val next: Option[Persistent.Partial]) extends Complete

    sealed trait Behind {
      def previous: Persistent.Partial
    }

    class BehindFetchNext(val previous: Persistent.Partial) extends InComplete with Behind
    class BehindStopped(val previous: Persistent.Partial) extends Complete with Behind

    val aheadOrNoneOrEndNone = new AheadOrNoneOrEnd(None)
    class AheadOrNoneOrEnd(val ahead: Option[Persistent.Partial]) extends Complete
  }

  sealed trait Bounded extends KeyMatcher

  sealed trait Get extends KeyMatcher {
    def matchOrStop: Get.MatchOnly =
      new Getter(
        key = key,
        matchOnly = true
      )(keyOrder)
  }

  object Get {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get =
      new Getter(
        key = key,
        matchOnly = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object MatchOnly {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get.MatchOnly =
        new Getter(
          key = key,
          matchOnly = true
        )(keyOrder)
    }

    sealed trait MatchOnly extends Bounded {
      def matchOnly: Boolean
    }
  }

  //private to disallow creating hashIndex Get from here.
  private class Getter(val key: Slice[Byte],
                       val matchOnly: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Get with Get.MatchOnly {

    override def apply(previous: Persistent.Partial,
                       next: Option[Persistent.Partial],
                       hasMore: => Boolean): KeyMatcher.Result =
      next.getOrElse(previous) match {
        case fixed: Persistent.Partial.Fixed =>
          val matchResult = keyOrder.compare(key, fixed.key)
          if (matchResult == 0)
            new Matched(next map (_ => previous), fixed, None)
          else if (matchResult > 0 && hasMore)
            if (matchOnly)
              new BehindStopped(fixed)
            else
              new BehindFetchNext(fixed)
          else
            new AheadOrNoneOrEnd(next orElse Some(previous))

        case range: Persistent.Partial.RangeT =>
          val fromKeyMatch = keyOrder.compare(key, range.fromKey)
          val toKeyMatch = keyOrder.compare(key, range.toKey)
          if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
            new Matched(next map (_ => previous), range, None)
          else if (toKeyMatch >= 0 && hasMore)
            if (matchOnly)
              new BehindStopped(range)
            else
              new BehindFetchNext(range)
          else
            new AheadOrNoneOrEnd(next orElse Some(previous))
      }
  }

  sealed trait Lower extends KeyMatcher {
    def matchOrStop: Lower.MatchOnly =
      new LowerMatcher(
        key = key,
        matchOnly = true
      )(keyOrder)
  }

  object Lower {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower =
      new LowerMatcher(
        key = key,
        matchOnly = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object MatchOnly {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower.MatchOnly =
        new LowerMatcher(
          key = key,
          matchOnly = true
        )(keyOrder)
    }

    sealed trait MatchOnly extends Bounded {
      def matchOnly: Boolean
    }
  }

  private class LowerMatcher(val key: Slice[Byte],
                             val matchOnly: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Lower with Lower.MatchOnly {

    override def apply(previous: Persistent.Partial,
                       next: Option[Persistent.Partial],
                       hasMore: => Boolean): KeyMatcher.Result =
      next match {
        case someNext @ Some(next) =>
          val nextCompare = keyOrder.compare(next.key, key)
          if (nextCompare >= 0)
            if (keyOrder.compare(previous.key, key) < 0)
              new Matched(None, previous, someNext)
            else
              new AheadOrNoneOrEnd(someNext)
          else if (nextCompare < 0)
            if (hasMore)
              next match {
                case range: Persistent.Partial.RangeT if keyOrder.compare(key, range.toKey) <= 0 =>
                  new Matched(Some(previous), next, None)

                case _ =>
                  if (matchOnly)
                    new BehindStopped(next)
                  else
                    new BehindFetchNext(next)
              }
            else
              new Matched(Some(previous), next, None)
          else
            new AheadOrNoneOrEnd(someNext)

        case None =>
          val previousCompare = keyOrder.compare(previous.key, key)
          if (previousCompare == 0)
            new AheadOrNoneOrEnd(Some(previous))
          else if (previousCompare < 0)
            if (hasMore)
              previous match {
                case range: Persistent.Partial.RangeT if keyOrder.compare(key, range.toKey) <= 0 =>
                  new Matched(None, previous, next)

                case _ =>
                  new BehindFetchNext(previous)
              }
            else
              new Matched(None, previous, next)
          else
            new AheadOrNoneOrEnd(Some(previous))
      }
  }

  sealed trait Higher extends KeyMatcher {
    def matchOrStop: Higher.MatchOnly =
      new HigherMatcher(
        key = key,
        matchOnly = true
      )(keyOrder)
  }

  object Higher {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher =
      new HigherMatcher(
        key = key,
        matchOnly = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object MatchOnly {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher.MatchOnly =
        new HigherMatcher(
          key = key,
          matchOnly = true
        )(keyOrder)
    }

    sealed trait MatchOnly extends Bounded {
      def matchOnly: Boolean
    }
  }

  private class HigherMatcher(val key: Slice[Byte],
                              val matchOnly: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Higher with Higher.MatchOnly {

    override def apply(previous: Persistent.Partial,
                       next: Option[Persistent.Partial],
                       hasMore: => Boolean): KeyMatcher.Result = {
      val keyValue = next getOrElse previous
      val nextCompare = keyOrder.compare(keyValue.key, key)
      if (nextCompare > 0)
        new Matched(next map (_ => previous), keyValue, None)
      else if (nextCompare <= 0)
        keyValue match {
          case range: Persistent.Partial.RangeT if keyOrder.compare(key, range.toKey) < 0 =>
            new Matched(next map (_ => previous), keyValue, None)

          case _ =>
            if (hasMore)
              if (matchOnly)
                new BehindStopped(keyValue)
              else
                new BehindFetchNext(keyValue)
            else
              new AheadOrNoneOrEnd(next orElse Some(previous))
        }
      else
        new AheadOrNoneOrEnd(next orElse Some(previous))
    }
  }
}
