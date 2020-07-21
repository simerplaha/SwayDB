package swaydb.core.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._

class ProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) = LimitHashMap[K, V](limit, 10)
}

class ProbeLimitConcurrentHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) = LimitHashMap.concurrent[K, V](limit, 10)
}

class NoProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) =
    eitherOne(
      //create directly with noProbe or via 0 or negative probe.
      LimitHashMap[K, V](limit),
      LimitHashMap[K, V](limit, -randomIntMax(2))
    )
}

class NoProbeLimitConcurrentHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) =
    eitherOne(
      //create directly with noProbe or via 0 or negative probe.
      LimitHashMap.concurrent[K, V](limit),
      LimitHashMap.concurrent[K, V](limit, -randomIntMax(2))
    )
}

sealed trait LimitHashMapSpec extends AnyFlatSpec with Matchers {

  def createMap[K, V >: Null](limit: Integer): LimitHashMap[K, V]

  it should "create empty map for invalid inputs" in {
    Seq(
      createMap[Integer, Integer](0),
      createMap[Integer, Integer](-1),
      createMap[Integer, Integer](Int.MinValue)
    ) foreach {
      map =>
        map.getOrNull(1) should be(null)
        map.put(1, 1)
        map.getOrNull(1) should be(null)
        map.size shouldBe 0
        map.limit shouldBe 0
        map.toList shouldBe empty
    }
  }

  it should "return none for empty map" in {
    val map = createMap[Integer, Integer](1)
    (1 to 100) foreach {
      i =>
        map.getOrNull(i) should be(null)
    }
  }

  it should "write to all slots" in {
    val map = createMap[Integer, Integer](10)

    (1 to 10) foreach {
      i =>
        map.put(i, i)
        map.getOrNull(i) shouldBe i
    }

    (1 to 10) foreach {
      i =>
        map.getOrNull(i) shouldBe i
    }

    map.toList.sorted shouldBe (1 to 10).map(i => (i, i))
  }

  it should "overwrite on overflow" in {

    val map = createMap[Integer, Integer](10)

    (1 to 10) foreach {
      i =>
        map.put(i, i)
    }

    map.toList.sorted shouldBe (1 to 10).map(i => (i, i))

    (11 to 20) foreach {
      i =>
        map.put(i, i)
        map.getOrNull(i) shouldBe i
    }

    (11 to 20) foreach {
      i =>
        map.getOrNull(i) shouldBe i
    }

    map.toList.sorted shouldBe (11 to 20).map(i => (i, i))

    (1 to 10) foreach {
      i =>
        map.getOrNull(i) should be(null)
    }
  }
}
