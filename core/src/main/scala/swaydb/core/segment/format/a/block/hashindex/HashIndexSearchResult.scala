package swaydb.core.segment.format.a.block.hashindex

import swaydb.IO
import swaydb.core.data.Persistent

sealed trait HashIndexSearchResult {
  def matched: Option[Persistent.Partial]
}

object HashIndexSearchResult {

  val none = None(scala.None, scala.None)

  val noneIO = IO.Right[Nothing, HashIndexSearchResult.None](None(scala.None, scala.None))(IO.ExceptionHandler.Nothing)

  case class None(lower: Option[Persistent.Partial], higher: Option[Persistent.Partial]) extends HashIndexSearchResult {
    def matched: Option[Persistent.Partial] = scala.None
  }

  case class Some(keyValue: Persistent.Partial) extends HashIndexSearchResult {
    def matched: Option[Persistent.Partial] = scala.Some(keyValue)
  }
}
