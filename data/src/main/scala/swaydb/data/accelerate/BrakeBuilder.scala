package swaydb.data.accelerate

import scala.concurrent.duration.FiniteDuration

class BrakeBuilder {
  private var increaseMapSizeOnMapCount: Int = _
  private var increaseMapSizeBy: Int = _
  private var maxMapSize: Long = _
  private var brakeOnMapCount: Int = _
  private var brakeFor: FiniteDuration = _
  private var releaseRate: FiniteDuration = _
  private var levelZeroMeter: LevelZeroMeter = _
}

object BrakeBuilder {

  class Step0(builder: BrakeBuilder) {
    def withIncreaseMapSizeOnMapCount(increaseMapSizeOnMapCount: Int) = {
      builder.increaseMapSizeOnMapCount = increaseMapSizeOnMapCount
      new Step1(builder)
    }
  }

  class Step1(builder: BrakeBuilder) {
    def withIncreaseMapSizeBy(increaseMapSizeBy: Int) = {
      builder.increaseMapSizeBy = increaseMapSizeBy
      new Step2(builder)
    }
  }

  class Step2(builder: BrakeBuilder) {
    def withMaxMapSize(maxMapSize: Long) = {
      builder.maxMapSize = maxMapSize
      new Step3(builder)
    }
  }

  class Step3(builder: BrakeBuilder) {
    def withBrakeOnMapCount(brakeOnMapCount: Int) = {
      builder.brakeOnMapCount = brakeOnMapCount
      new Step4(builder)
    }
  }

  class Step4(builder: BrakeBuilder) {
    def withBrakeFor(brakeFor: FiniteDuration) = {
      builder.brakeFor = brakeFor
      new Step5(builder)
    }
  }

  class Step5(builder: BrakeBuilder) {
    def withReleaseRate(releaseRate: FiniteDuration) = {
      builder.releaseRate = releaseRate
      new Step6(builder)
    }
  }

  class Step6(builder: BrakeBuilder) {
    def withLevelZeroMeter(levelZeroMeter: LevelZeroMeter) = {
      builder.levelZeroMeter = levelZeroMeter
      new Step7(builder)
    }
  }

  class Step7(builder: BrakeBuilder) {
    def build(): Accelerator =
      Accelerator.brake(
        increaseMapSizeOnMapCount = builder.increaseMapSizeOnMapCount,
        increaseMapSizeBy = builder.increaseMapSizeBy,
        maxMapSize = builder.maxMapSize,
        brakeOnMapCount = builder.brakeOnMapCount,
        brakeFor = builder.brakeFor,
        releaseRate = builder.releaseRate
      )(builder.levelZeroMeter)
  }

  def builder() = new Step0(new BrakeBuilder())
}
