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

object MatchResult {
  case class Matched(result: Persistent) extends MatchResult {
    override def asIO: IO.Success[MatchResult] =
      IO.Success(this)
  }
  case object Behind extends MatchResult {
    val asIO = IO.Success(this)
  }
  case object BehindStopped extends MatchResult {
    val asIO = IO.Success(this)
  }
  case object AheadOrEnd extends MatchResult {
    val asIO = IO.Success(this)
  }
}

private[core] sealed trait KeyMatcher {
  def key: Slice[Byte]

  def apply(previous: Persistent,
            next: Option[Persistent],
            hasMore: => Boolean): MatchResult

  def keyOrder: KeyOrder[Slice[Byte]]
}

private[core] object KeyMatcher {
  sealed trait Get extends KeyMatcher {
    def whilePrefixCompressed =
      Get.WhilePrefixCompressed(key)(keyOrder)
  }

  object Get {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get =
      Getter(
        key = key,
        whileNextIsPrefixCompressed = false
      )

    /**
      * Reserved for [[SegmentSearcher]] only. Other clients should just submit [[Get]]
      * and [[SegmentSearcher]] should apply HashIndex check if necessary.
      */

    object WhilePrefixCompressed {
      private[a] def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Get.WhilePrefixCompressed =
        Getter(
          key = key,
          whileNextIsPrefixCompressed = true
        )
    }
    sealed trait WhilePrefixCompressed extends KeyMatcher {
      def whileNextIsPrefixCompressed: Boolean
    }
  }

  //private to disallow creating hashIndex Get from here.
  private case class Getter(key: Slice[Byte],
                            whileNextIsPrefixCompressed: Boolean)(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends Get with Get.WhilePrefixCompressed {

    def shouldFetchNext(next: Option[Persistent]) =
      !whileNextIsPrefixCompressed || next.forall(_.isPrefixCompressed)

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult =
      next.getOrElse(previous) match {
        case fixed: Persistent.Fixed =>
          val matchResult = keyOrder.compare(key, fixed.key)
          if (matchResult == 0)
            Matched(fixed)
          else if (matchResult > 0 && hasMore)
            if (shouldFetchNext(next))
              Behind
            else
              BehindStopped
          else
            AheadOrEnd

        case group: Persistent.Group =>
          val fromKeyMatch = keyOrder.compare(key, group.minKey)
          val toKeyMatch: Int = keyOrder.compare(key, group.maxKey.maxKey)
          if (fromKeyMatch >= 0 && ((group.maxKey.inclusive && toKeyMatch <= 0) || (!group.maxKey.inclusive && toKeyMatch < 0))) //is within the range
            Matched(group)
          else if (toKeyMatch >= 0 && hasMore)
            if (shouldFetchNext(next))
              Behind
            else
              BehindStopped
          else
            AheadOrEnd

        case range: Persistent.Range =>
          val fromKeyMatch = keyOrder.compare(key, range.fromKey)
          val toKeyMatch = keyOrder.compare(key, range.toKey)
          if (fromKeyMatch >= 0 && toKeyMatch < 0) //is within the range
            Matched(range)
          else if (toKeyMatch >= 0 && hasMore)
            if (shouldFetchNext(next))
              Behind
            else
              BehindStopped
          else
            AheadOrEnd
      }
  }

  case class Lower(key: Slice[Byte])(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends KeyMatcher {

    override def apply(previous: Persistent,
                       next: Option[Persistent],
                       hasMore: => Boolean): MatchResult =
      next match {
        case Some(next) =>
          val nextCompare = keyOrder.compare(next.key, key)
          if (nextCompare >= 0)
            if (keyOrder.compare(previous.key, key) < 0)
              Matched(previous)
            else
              AheadOrEnd
          else if (nextCompare < 0)
            if (hasMore)
              next match {
                case range: Persistent.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(next)

                case group: Persistent.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
                  Matched(next)

                case _ =>
                  Behind
              }
            else
              Matched(next)
          else
            AheadOrEnd

        case None =>
          val previousCompare = keyOrder.compare(previous.key, key)
          if (previousCompare == 0)
            AheadOrEnd
          else if (previousCompare < 0)
            if (hasMore)
              previous match {
                case range: Persistent.Range if keyOrder.compare(key, range.toKey) <= 0 =>
                  Matched(previous)

                case group: Persistent.Group if keyOrder.compare(key, group.minKey) > 0 && keyOrder.compare(key, group.maxKey.maxKey) <= 0 =>
                  Matched(previous)

                case _ =>
                  Behind
              }
            else
              Matched(previous)
          else
            AheadOrEnd
      }
  }

  case class Higher(key: Slice[Byte])(implicit val keyOrder: KeyOrder[Slice[Byte]]) extends KeyMatcher {

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
              Behind
            else
              AheadOrEnd
        }
      else
        AheadOrEnd
    }
  }

}
