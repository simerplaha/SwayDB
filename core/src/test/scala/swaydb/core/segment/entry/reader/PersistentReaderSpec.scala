package swaydb.core.segment.entry.reader

import org.scalatest.OptionValues._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.segment.entry.reader.base.BaseEntryReader4

class PersistentReaderSpec extends AnyWordSpec with Matchers {

  "populateBaseEntryIds" should {
    PersistentReader.populateBaseEntryIds()
    val ids = PersistentReader.getBaseEntryIds()

    "be defined" in {
      ids shouldBe defined //should exist
      ids should not be empty //should not be empty
      ids.value.foreach(_ should not be null) //should not contain null entry
    }

    "insert ids incrementally" in {
      //all ids are added incrementally.
      ids.value.zipWithIndex.foldLeft(0) {
        case (expected, (actual, index)) =>
          actual should not be null
          expected shouldBe index
          expected + 1
      }
    }

    "not contain any duplicates" in {
      val distinctIds = ids.value.distinct
      distinctIds should have size ids.value.length
      distinctIds should have size BaseEntryReader4.maxID + 1
    }
  }
}
