package swaydb.core.skiplist

import swaydb.testkit.TestKit.eitherOne

object SkipListTestKit {

  def randomAtomicRangesAction(): AtomicRanges.Action =
    eitherOne(AtomicRanges.Action.Write, new AtomicRanges.Action.Read())

}
