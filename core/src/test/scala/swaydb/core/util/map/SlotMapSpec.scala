package swaydb.core.util.map

import org.scalatest.OptionValues._
import org.scalatest.{FlatSpec, Matchers}

class SlotMapSpec extends FlatSpec with Matchers {

  it should "assign all slots" in {
    //    val slot = SlotMap[Int, Int](10)
    val slot = LimitHashMap[Int, Int](10)

    (1 to 10) foreach {
      i =>
        slot.put(i, i)
        slot.get(i).value shouldBe i
    }

    (1 to 10) foreach {
      i =>
        slot.get(i).value shouldBe i
    }

    (11 to 20) foreach {
      i =>
        slot.put(i, i)
        slot.get(i).value shouldBe i
    }

    (11 to 20) foreach {
      i =>
        slot.get(i).value shouldBe i
    }
  }
}
