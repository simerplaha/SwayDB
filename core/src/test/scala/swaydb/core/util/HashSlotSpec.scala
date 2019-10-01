package swaydb.core.util

import org.scalatest.{FlatSpec, Matchers}

class HashSlotSpec extends FlatSpec with Matchers {

  it should "assign all slots" in {
    val slot = HashSlot[Int, Int](10)

    (1 to 10) foreach {
      i =>
        slot.put(i, i)
    }

    slot foreach {
      i =>
        i should not be 0
    }
  }
}
