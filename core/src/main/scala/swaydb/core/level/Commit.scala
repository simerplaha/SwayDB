package swaydb.core.level

import swaydb.core.segment.Segment

sealed trait Commit
object Commit {
  case object Committed extends Commit
  case object Uncommitted extends Commit
  case class PartiallyCommitted(committed: Iterable[Segment], uncommitted: Iterable[Segment]) extends Commit
}
