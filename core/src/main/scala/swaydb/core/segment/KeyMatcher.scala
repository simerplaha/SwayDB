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
 */

package swaydb.core.segment

import swaydb.core.data.Persistent
import swaydb.core.segment.KeyMatcher.Result.{AheadOrNoneOrEnd, BehindFetchNext, BehindStopped, Matched}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] sealed trait KeyMatcher {
  def key: Slice[Byte]

  def apply(previous: Persistent.Partial,
            next: Persistent.PartialOption,
            hasMore: Boolean): KeyMatcher.Result

  def keyOrder: KeyOrder[Slice[Byte]]

  def matchOnly: Boolean
}

private[core] object KeyMatcher {

  sealed trait Result
  object Result {

    sealed trait Complete extends Result
    sealed trait InComplete extends Result

    class Matched(val result: Persistent.Partial) extends Complete

    sealed trait Behind
    final object BehindFetchNext extends InComplete with Behind

    /**
     * Used as outcome for matchOnly searches like *seekOne functions.
     * This result indicates that seek is behind and could continue but was stopped early.
     */
    final object BehindStopped extends Complete with Behind
    final object AheadOrNoneOrEnd extends Complete
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

    def matchMutateForBinarySearch(key: Slice[Byte],
                                   partialKeyValue: Persistent.Partial.Fixed)(implicit keyOrder: KeyOrder[Slice[Byte]]): Unit = {
      val matchResult = keyOrder.compare(key, partialKeyValue.key)
      if (matchResult == 0)
        partialKeyValue.isBinarySearchMatched = true
      else if (matchResult > 0)
        partialKeyValue.isBinarySearchBehind = true
      else
        partialKeyValue.isBinarySearchAhead = true
    }

    def matchMutateForBinarySearch(key: Slice[Byte],
                                   range: Persistent.Partial.Range)(implicit keyOrder: KeyOrder[Slice[Byte]]): Unit = {
      val fromKeyMatch = keyOrder.compare(key, range.fromKey)
      var compared: Boolean = false
      var toKeyCompare: Int = 0

      def toKeyMatch: Int =
        if (compared) {
          toKeyCompare
        } else {
          toKeyCompare = keyOrder.compare(key, range.toKey)
          compared = true
          toKeyCompare
        }

      if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
        range.isBinarySearchMatched = true
      else if (toKeyMatch >= 0)
        range.isBinarySearchBehind = true
      else
        range.isBinarySearchAhead = true
    }

    def matchForHashIndex(key: Slice[Byte],
                          partialKeyValue: Persistent.Partial.Fixed)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
      keyOrder.equiv(key, partialKeyValue.key)

    def matchForHashIndex(key: Slice[Byte],
                          range: Persistent.Partial.Range)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
      val fromKeyMatch = keyOrder.compare(key, range.fromKey)
      fromKeyMatch == 0 || (fromKeyMatch > 0 && keyOrder.lt(key, range.toKey))
    }
  }

  //private to disallow creating hashIndex Get from here.
  private class Getter(val key: Slice[Byte],
                       val matchOnly: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Get with Get.MatchOnly {

    override def apply(previous: Persistent.Partial,
                       next: Persistent.PartialOption,
                       hasMore: Boolean): KeyMatcher.Result =
      next.getOrElseC(previous) match {
        case fixed: Persistent.Partial.Fixed =>
          val matchResult = keyOrder.compare(key, fixed.key)
          if (matchResult == 0)
            new Matched(fixed)
          else if (matchResult > 0 && hasMore)
            if (matchOnly)
              BehindStopped
            else
              BehindFetchNext
          else
            AheadOrNoneOrEnd

        case range: Persistent.Partial.Range =>
          val fromKeyMatch = keyOrder.compare(key, range.fromKey)
          val toKeyMatch = keyOrder.compare(key, range.toKey)
          if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
            new Matched(range)
          else if (toKeyMatch >= 0 && hasMore)
            if (matchOnly)
              BehindStopped
            else
              BehindFetchNext
          else
            AheadOrNoneOrEnd
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
                       next: Persistent.PartialOption,
                       hasMore: Boolean): KeyMatcher.Result =
      next match {
        case next: Persistent.Partial =>
          val nextCompare = keyOrder.compare(next.key, key)
          if (nextCompare >= 0)
            if (keyOrder.compare(previous.key, key) < 0)
              new Matched(previous)
            else
              AheadOrNoneOrEnd
          else if (nextCompare < 0)
            if (hasMore)
              next match {
                case range: Persistent.Partial.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  new Matched(next)

                case _ =>
                  if (matchOnly)
                    BehindStopped
                  else
                    BehindFetchNext
              }
            else
              new Matched(next)
          else
            AheadOrNoneOrEnd

        case Persistent.Partial.Null =>
          val previousCompare = keyOrder.compare(previous.key, key)
          if (previousCompare == 0)
            AheadOrNoneOrEnd
          else if (previousCompare < 0)
            if (hasMore)
              previous match {
                case range: Persistent.Partial.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  new Matched(previous)

                case _ =>
                  BehindFetchNext
              }
            else
              new Matched(previous)
          else
            AheadOrNoneOrEnd
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
                       next: Persistent.PartialOption,
                       hasMore: Boolean): KeyMatcher.Result = {
      val keyValue = next getOrElseC previous
      val nextCompare = keyOrder.compare(keyValue.key, key)
      if (nextCompare > 0)
        new Matched(keyValue)
      else if (nextCompare <= 0)
        keyValue match {
          case range: Persistent.Partial.Range if keyOrder.compare(key, range.toKey) < 0 =>
            new Matched(keyValue)

          case _ =>
            if (hasMore)
              if (matchOnly)
                BehindStopped
              else
                BehindFetchNext
            else
              AheadOrNoneOrEnd
        }
      else
        AheadOrNoneOrEnd
    }
  }
}
