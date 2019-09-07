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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.data.Persistent
import swaydb.core.data.Persistent.Partial
import swaydb.core.segment.format.a.block.KeyMatcher.Result.{AheadOrNoneOrEnd, BehindFetchNext, BehindStopped, Matched}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] sealed trait KeyMatcher {
  def key: Slice[Byte]

  def apply(previous: Persistent.Partial,
            next: Option[Persistent.Partial],
            hasMore: => Boolean): KeyMatcher.Result

  def keyOrder: KeyOrder[Slice[Byte]]

  def seekOne: Boolean

  def whileNextIsPrefixCompressed: Boolean

  def shouldFetchNext(next: Option[Persistent.Partial]) =
    KeyMatcher.shouldFetchNext(this, next)
}

private[core] object KeyMatcher {

  sealed trait Result
  object Result {

    sealed trait Complete extends Result
    sealed trait InComplete extends Result

    case class Matched(previous: Option[Persistent.Partial], result: Persistent.Partial, next: Option[Persistent.Partial]) extends Complete

    sealed trait Behind {
      def previous: Persistent.Partial
    }
    case class BehindFetchNext(previous: Persistent.Partial) extends InComplete with Behind
    case class BehindStopped(previous: Persistent.Partial) extends Complete with Behind
    case object AheadOrNoneOrEnd extends Complete
  }

  def shouldFetchNext(matcher: KeyMatcher, next: Option[Persistent.Partial]) =
    (!matcher.seekOne || next.isEmpty) && (!matcher.whileNextIsPrefixCompressed || next.forall(_.isPrefixCompressed))

  sealed trait Bounded extends KeyMatcher

  sealed trait Get extends KeyMatcher {
    def boundWhilePrefixCompressed: Get.WhilePrefixCompressed =
      new Getter(
        key = key,
        whileNextIsPrefixCompressed = true,
        seekOne = false
      )(keyOrder)

    def matchOrStop: Get.SeekOne =
      new Getter(
        key = key,
        whileNextIsPrefixCompressed = false,
        seekOne = true
      )(keyOrder)
  }

  object Get {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get =
      new Getter(
        key = key,
        whileNextIsPrefixCompressed = false,
        seekOne = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object WhilePrefixCompressed {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get.WhilePrefixCompressed =
        new Getter(
          key = key,
          seekOne = false,
          whileNextIsPrefixCompressed = true
        )(keyOrder)
    }
    sealed trait WhilePrefixCompressed extends Bounded {
      def whileNextIsPrefixCompressed: Boolean
    }

    object SeekOne {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get.SeekOne =
        new Getter(
          key = key,
          seekOne = true,
          whileNextIsPrefixCompressed = false
        )(keyOrder)
    }

    sealed trait SeekOne extends Bounded {
      def seekOne: Boolean
    }
  }

  //private to disallow creating hashIndex Get from here.
  private class Getter(val key: Slice[Byte],
                       val seekOne: Boolean,
                       val whileNextIsPrefixCompressed: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Get with Get.WhilePrefixCompressed with Get.SeekOne {

    override def apply(previous: Persistent.Partial,
                       next: Option[Persistent.Partial],
                       hasMore: => Boolean): KeyMatcher.Result =
      next.getOrElse(previous) match {
        case fixed: Persistent.Partial.Fixed =>
          val matchResult = keyOrder.compare(key, fixed.key)
          if (matchResult == 0)
            Matched(next map (_ => previous), fixed, None)
          else if (matchResult > 0 && hasMore)
            if (shouldFetchNext(next))
              BehindFetchNext(fixed)
            else
              BehindStopped(fixed)
          else
            AheadOrNoneOrEnd

        case group: Persistent.Partial.Group =>
          val fromKeyMatch = keyOrder.compare(key, group.minKey)
          val toKeyMatch: Int = keyOrder.compare(key, group.maxKey.maxKey)
          if (fromKeyMatch >= 0 && ((group.maxKey.inclusive && toKeyMatch <= 0) || (!group.maxKey.inclusive && toKeyMatch < 0))) //is within the range
            Matched(next map (_ => previous), group, None)
          else if (toKeyMatch >= 0 && hasMore)
            if (shouldFetchNext(next))
              BehindFetchNext(group)
            else
              BehindStopped(group)
          else
            AheadOrNoneOrEnd

        case range: Persistent.Partial.Range =>
          val fromKeyMatch = keyOrder.compare(key, range.fromKey)
          val toKeyMatch = keyOrder.compare(key, range.toKey)
          if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
            Matched(next map (_ => previous), range, None)
          else if (toKeyMatch >= 0 && hasMore)
            if (shouldFetchNext(next))
              BehindFetchNext(range)
            else
              BehindStopped(range)
          else
            AheadOrNoneOrEnd
      }
  }

  sealed trait Lower extends KeyMatcher {
    def boundWhilePrefixCompressed: Lower.WhilePrefixCompressed =
      new LowerMatcher(
        key = key,
        whileNextIsPrefixCompressed = true,
        seekOne = false
      )(keyOrder)

    def matchOrStop: Lower.SeekOne =
      new LowerMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        seekOne = true
      )(keyOrder)
  }

  object Lower {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower =
      new LowerMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        seekOne = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object WhilePrefixCompressed {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower.WhilePrefixCompressed =
        new LowerMatcher(
          key = key,
          seekOne = false,
          whileNextIsPrefixCompressed = true
        )(keyOrder)
    }
    sealed trait WhilePrefixCompressed extends Bounded {
      def whileNextIsPrefixCompressed: Boolean
    }

    object SeekOne {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Lower.SeekOne =
        new LowerMatcher(
          key = key,
          seekOne = true,
          whileNextIsPrefixCompressed = false
        )(keyOrder)
    }

    sealed trait SeekOne extends Bounded {
      def seekOne: Boolean
    }
  }

  private class LowerMatcher(val key: Slice[Byte],
                             val seekOne: Boolean,
                             val whileNextIsPrefixCompressed: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Lower with Lower.WhilePrefixCompressed with Lower.SeekOne {

    override def apply(previous: Persistent.Partial,
                       next: Option[Persistent.Partial],
                       hasMore: => Boolean): KeyMatcher.Result =
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
                case range: Persistent.Partial.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(Some(previous), next, None)

                case group: Persistent.Partial.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
                  Matched(Some(previous), next, None)

                case _ =>
                  if (shouldFetchNext(someNext))
                    BehindFetchNext(next)
                  else
                    BehindStopped(next)
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
                case range: Persistent.Partial.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(None, previous, next)

                case group: Persistent.Partial.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
                  Matched(None, previous, next)

                case _ =>
                  if (shouldFetchNext(None))
                    BehindFetchNext(previous)
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
        seekOne = false
      )(keyOrder)

    def matchOrStop: Higher.SeekOne =
      new HigherMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        seekOne = true
      )(keyOrder)
  }

  object Higher {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher =
      new HigherMatcher(
        key = key,
        whileNextIsPrefixCompressed = false,
        seekOne = false
      )

    sealed trait Bounded extends KeyMatcher.Bounded

    object WhilePrefixCompressed {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher.WhilePrefixCompressed =
        new HigherMatcher(
          key = key,
          seekOne = false,
          whileNextIsPrefixCompressed = true
        )(keyOrder)
    }
    sealed trait WhilePrefixCompressed extends Bounded {
      def whileNextIsPrefixCompressed: Boolean
    }

    object SeekOne {
      def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Higher.SeekOne =
        new HigherMatcher(
          key = key,
          seekOne = true,
          whileNextIsPrefixCompressed = false
        )(keyOrder)
    }

    sealed trait SeekOne extends Bounded {
      def seekOne: Boolean
    }
  }

  private class HigherMatcher(val key: Slice[Byte],
                              val seekOne: Boolean,
                              val whileNextIsPrefixCompressed: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Higher with Higher.WhilePrefixCompressed with Higher.SeekOne {

    override def apply(previous: Persistent.Partial,
                       next: Option[Persistent.Partial],
                       hasMore: => Boolean): KeyMatcher.Result = {
      val keyValue = next getOrElse previous
      val nextCompare = keyOrder.compare(keyValue.key, key)
      if (nextCompare > 0)
        Matched(next map (_ => previous), keyValue, None)
      else if (nextCompare <= 0)
        keyValue match {
          case range: Persistent.Partial.Range if keyOrder.compare(key, range.toKey) < 0 =>
            Matched(next map (_ => previous), keyValue, None)

          case group: Persistent.Partial.Group if keyOrder.compare(key, group.maxKey.maxKey) < 0 =>
            Matched(next map (_ => previous), keyValue, None)

          case _ =>
            if (hasMore)
              if (shouldFetchNext(next))
                BehindFetchNext(keyValue)
              else
                BehindStopped(keyValue)
            else
              AheadOrNoneOrEnd
        }
      else
        AheadOrNoneOrEnd
    }
  }
}
