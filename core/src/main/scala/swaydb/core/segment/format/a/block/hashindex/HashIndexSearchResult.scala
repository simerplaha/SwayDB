package swaydb.core.segment.format.a.block.hashindex

import swaydb.core.data.Persistent

sealed trait HashIndexSearchResult {
  def matched: Option[Persistent.Partial]
}

object HashIndexSearchResult {

  sealed trait NotFound extends HashIndexSearchResult {
    def lower: Option[Persistent.Partial]
    def matched: Option[Persistent.Partial] = Option.empty
  }

  case object None extends NotFound {
    override final val lower = Option.empty
  }

  case class Lower(keyValue: Persistent.Partial) extends NotFound {
    override def lower: Option[Persistent.Partial] = Some(keyValue)
  }

  case class Found(keyValue: Persistent.Partial) extends HashIndexSearchResult {
    def matched: Option[Persistent.Partial] = Some(keyValue)
  }
}
