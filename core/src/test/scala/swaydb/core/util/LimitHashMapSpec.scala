package swaydb.core.util

import org.scalatest.OptionValues._
import org.scalatest.{FlatSpec, Matchers}

class ProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V](limit: Int) = LimitHashMap[K, V](limit, 10)
}

class NoProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V](limit: Int) = LimitHashMap[K, V](limit)
}

sealed trait LimitHashMapSpec extends FlatSpec with Matchers {

  def createMap[K, V](limit: Int): LimitHashMap[K, V]

  it should "not fail if limit is <= 0" in {
    val zeroMap = createMap[Int, Int](0)
    zeroMap.get(1) shouldBe empty
    zeroMap.size shouldBe 0

    val negativeMap = createMap[Int, Int](-1)
    negativeMap.get(1) shouldBe empty
    negativeMap.size shouldBe 0
  }

  it should "return none for empty" in {
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
