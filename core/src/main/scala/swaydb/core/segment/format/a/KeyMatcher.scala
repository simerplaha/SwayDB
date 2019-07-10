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

package swaydb.core.segment.format.a

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.MatchResult.{Matched, _}
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

sealed trait MatchResult {
  def asIO: IO.Success[MatchResult]
}
sealed trait FinishedMatchResult extends MatchResult {
  def asIO: IO.Success[FinishedMatchResult]
}

object MatchResult {
  case class Matched(previous: Option[Persistent], result: Persistent, next: Option[Persistent]) extends FinishedMatchResult {
    override def asIO: IO.Success[FinishedMatchResult] =
      IO.Success(this)
  }
  case object BehindFetchNext extends MatchResult {
    val asIO = IO.Success(this)
  }
  case object BehindStopped extends FinishedMatchResult {
    val asIO = IO.Success(this)
  }
  case object AheadOrNoneOrEnd extends FinishedMatchResult {
    val asIO = IO.Success(this)
  }
}

private[core] sealed trait KeyMatcher {
  def key: Slice[Byte]

  def apply(previous: Persistent,
            next: Option[Persistent],
            hasMore: => Boolean): MatchResult

  def keyOrder: KeyOrder[Slice[Byte]]

  def matchOnly: Boolean

  def whileNextIsPrefixCompressed: Boolean

  def shouldFetchNext(next: Option[Persistent]) =
    KeyMatcher.shouldFetchNext(this, next)
}

private[core] object KeyMatcher {

  def shouldFetchNext(matcher: KeyMatcher, next: Option[Persistent]) =
    !matcher.matchOnly && (!matcher.whileNextIsPrefixCompressed || next.forall(_.isPrefixCompressed))

  sealed trait Bounded extends KeyMatcher

  sealed trait Get extends KeyMatcher {
    def boundWhilePrefixCompressed: Get.WhilePrefixCompressed =
      new Getter(
        key = key,
        whileNextIsPrefixCompressed = true,
        matchOnly = false
      )(keyOrder)

    def matchOrStop: Get.MatchOnly =
      new Getter(
        key = key,
        whileNextIsPrefixCompressed = false,
        matchOnly = true
      )(keyOrder)
  }

  object Get {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get =
      new Getter(
        key = key,
        whileNextIsPrefixCompressed = false,
        matchOnly = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object WhilePrefixCompressed {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get.WhilePrefixCompressed =
        new Getter(
          key = key,
          matchOnly = false,
          whileNextIsPrefixCompressed = true
        )(keyOrder)
    }
    sealed trait WhilePrefixCompressed extends Bounded {
      def whileNextIsPrefixCompressed: Boolean
    }

    object MatchOnly {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get.MatchOnly =
        new Getter(
          key = key,
          matchOnly = true,
          whileNextIsPrefixCompressed = false
        )(keyOrder)
    }

    sealed trait MatchOnly extends Bounded {
      def matchOnly: Boolean
    }
  }

  //private to disallow creating hashIndex Get from here.
  private class Getter(val key: Slice[Byte],
                       val matchOnly: Boolean,
                       val whileNextIsPrefixCompressed: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Get with Get.WhilePrefixCompressed with Get.MatchOnly {

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult =
      next.getOrElse(previous) match {
        case fixed: Persistent.Fixed =>
          val matchResult = keyOrder.compare(key, fixed.key)
          if (matchResult == 0)
            Matched(next map (_ => previous), fixed, None)
          else if (matchResult > 0 && hasMore)
            if (shouldFetchNext(next))
              BehindFetchNext
            else
              BehindStopped
          else
            AheadOrNoneOrEnd

        case group: Persistent.Group =>
          val fromKeyMatch = keyOrder.compare(key, group.minKey)
          val toKeyMatch: Int = keyOrder.compare(key, group.maxKey.maxKey)
          if (fromKeyMatch >= 0 && ((group.maxKey.inclusive && toKeyMatch <= 0) || (!group.maxKey.inclusive && toKeyMatch < 0))) //is within the range
            Matched(next map (_ => previous), group, None)
          else if (toKeyMatch >= 0 && hasMore)
            if (shouldFetchNext(next))
              BehindFetchNext
            else
              BehindStopped
          else
            AheadOrNoneOrEnd

        case range: Persistent.Range =>
          val fromKeyMatch = keyOrder.compare(key, range.fromKey)
          val toKeyMatch = keyOrder.compare(key, range.toKey)
          if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
            Matched(next map (_ => previous), range, None)
          else if (toKeyMatch >= 0 && hasMore)
            if (shouldFetchNext(next))
              BehindFetchNext
            else
              BehindStopped
          else
            AheadOrNoneOrEnd
      }
  }

  sealed trait Lower extends KeyMatcher {
    def boundWhilePrefixCompressed: Lower.WhilePrefixCompressed =
      new LowerMatcher(
        key = key,
        whileNextIsPrefixCompressed = true,
        matchOnly = false
      )(keyOrder)

    def matchOrStop: Lower.MatchOnly =
      new LowerMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        matchOnly = true
      )(keyOrder)
  }

  object Lower {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower =
      new LowerMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        matchOnly = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object WhilePrefixCompressed {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower.WhilePrefixCompressed =
        new LowerMatcher(
          key = key,
          matchOnly = false,
          whileNextIsPrefixCompressed = true
        )(keyOrder)
    }
    sealed trait WhilePrefixCompressed extends Bounded {
      def whileNextIsPrefixCompressed: Boolean
    }

    object MatchOnly {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower.MatchOnly =
        new LowerMatcher(
          key = key,
          matchOnly = true,
          whileNextIsPrefixCompressed = false
        )(keyOrder)
    }

    sealed trait MatchOnly extends Bounded {
      def matchOnly: Boolean
    }
  }

  private class LowerMatcher(val key: Slice[Byte],
                             val matchOnly: Boolean,
                             val whileNextIsPrefixCompressed: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Lower with Lower.WhilePrefixCompressed with Lower.MatchOnly {

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult =
      next match {
        case someNext @ Some(next) =>
          val nextCompare = keyOrder.compare(next.key, key)
          if (nextCompare >= 0)
            if (keyOrder.compare(previous.key, key) < 0)
              Matched(None, previous, someNext)
            else
              AheadOrNoneOrEnd
          else if (nextCompare < 0)
            if (hasMore)
              next match {
                case range: Persistent.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(Some(previous), next, None)

                case group: Persistent.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
                  Matched(Some(previous), next, None)

                case _ =>
                  if (shouldFetchNext(someNext))
                    BehindFetchNext
                  else
                    BehindStopped
              }
            else
              Matched(Some(previous), next, None)
          else
            AheadOrNoneOrEnd

        case None =>
          val previousCompare = keyOrder.compare(previous.key, key)
          if (previousCompare == 0)
            AheadOrNoneOrEnd
          else if (previousCompare < 0)
            if (hasMore)
              previous match {
                case range: Persistent.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(None, previous, next)

                case group: Persistent.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
                  Matched(None, previous, next)

                case _ =>
                  if (shouldFetchNext(None))
                    BehindFetchNext
                  else
                    Matched(None, previous, next) //if fetching next is not allowed then lower is the currently known lower.
              }
            else
              Matched(None, previous, next)
          else
            AheadOrNoneOrEnd
      }
  }

  sealed trait Higher extends KeyMatcher {
    def boundWhilePrefixCompressed: Higher.WhilePrefixCompressed =
      new HigherMatcher(
        key = key,
        whileNextIsPrefixCompressed = true,
        matchOnly = false
      )(keyOrder)

    def matchOrStop: Higher.MatchOnly =
      new HigherMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        matchOnly = true
      )(keyOrder)
  }

  object Higher {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher =
      new HigherMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        matchOnly = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object WhilePrefixCompressed {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher.WhilePrefixCompressed =
        new HigherMatcher(
          key = key,
          matchOnly = false,
          whileNextIsPrefixCompressed = true
        )(keyOrder)
    }
    sealed trait WhilePrefixCompressed extends Bounded {
      def whileNextIsPrefixCompressed: Boolean
    }

    object MatchOnly {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher.MatchOnly =
        new HigherMatcher(
          key = key,
          matchOnly = true,
          whileNextIsPrefixCompressed = false
        )(keyOrder)
    }

    sealed trait MatchOnly extends Bounded {
      def matchOnly: Boolean
    }
  }

  private class HigherMatcher(val key: Slice[Byte],
                              val matchOnly: Boolean,
                              val whileNextIsPrefixCompressed: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Higher with Higher.WhilePrefixCompressed with Higher.MatchOnly {

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult = {
      val keyValue = next getOrElse previous
      val nextCompare = keyOrder.compare(keyValue.key, key)
      if (nextCompare > 0)
        Matched(next map (_ => previous), keyValue, None)
      else if (nextCompare <= 0)
        keyValue match {
          case range: Persistent.Range if keyOrder.compare(key, range.toKey) < 0 =>
            Matched(next map (_ => previous), keyValue, None)

          case group: Persistent.Group if keyOrder.compare(key, group.maxKey.maxKey) < 0 =>
            Matched(next map (_ => previous), keyValue, None)

          case _ =>
            if (hasMore)
              if (shouldFetchNext(next))
                BehindFetchNext
              else
                BehindStopped
            else
              AheadOrNoneOrEnd
        }
      else
        AheadOrNoneOrEnd
    }
  }
}
