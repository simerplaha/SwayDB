package swaydb.core.util

import org.scalatest.{FlatSpec, Matchers}

class IDGeneratorSpec extends FlatSpec with Matchers {

  it should "always return new incremental ids when access concurrently" in {

    val gen = IDGenerator()

    (1 to 100).par.foldLeft(-1L) {
      case (previous, _) =>
        //check nextSegmentId should return valid text
        gen.nextSegmentID should fullyMatch regex s"\\d+\\.seg"
        val next = gen.nextID
        next should be > previous
        next
    }
  }

  it should "return segment string" in {
    IDGenerator.segmentId(1) should fullyMatch regex "1.seg"
  }

}
