package swaydb.core.util

import org.scalatest.OptionValues._
import org.scalatest.{FlatSpec, Matchers}
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._

class ProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V](limit: Int) = LimitHashMap[K, V](limit, 10)
}

class NoProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V](limit: Int) =
    eitherOne(
      //create directly with noProbe or via 0 or negative probe.
      LimitHashMap[K, V](limit),
      LimitHashMap[K, V](limit, -randomIntMax(2))
    )
}

sealed trait LimitHashMapSpec extends FlatSpec with Matchers {

  def createMap[K, V](limit: Int): LimitHashMap[K, V]

  it should "create empty map for invalid inputs" in {
    Seq(
      createMap[Int, Int](0),
      createMap[Int, Int](-1),
      createMap[Int, Int](Int.MinValue)
    ) foreach {
      map =>
        map.get(1) shouldBe empty
        map.put(1, 1)
        map.get(1) shouldBe empty
        map.size shouldBe 0
        map.limit shouldBe 0
        map.toList shouldBe empty
    }
  }

  it should "return none for empty map" in {
    val map = createMap[Int, Int](1)
    (1 to 100) foreach {
      i =>
        map.get(i) shouldBe empty
    }
  }

  it should "write to all slots" in {
    val map = createMap[Int, Int](10)

    (1 to 10) foreach {
      i =>
        map.put(i, i)
        map.get(i).value shouldBe i
    }

    (1 to 10) foreach {
      i =>
        map.get(i).value shouldBe i
    }

    map.toList.sorted shouldBe (1 to 10).map(i => (i, i))
  }

  it should "overwrite on overflow" in {

    val map = createMap[Int, Int](10)

    (1 to 10) foreach {
      i =>
        map.put(i, i)
    }

    map.toList.sorted shouldBe (1 to 10).map(i => (i, i))

    (11 to 20) foreach {
      i =>
        map.put(i, i)
        map.get(i).value shouldBe i
    }

    (11 to 20) foreach {
      i =>
        map.get(i).value shouldBe i
    }

    map.toList.sorted shouldBe (11 to 20).map(i => (i, i))

    (1 to 10) foreach {
      i =>
        map.get(i) shouldBe empty
    }
  }
}
