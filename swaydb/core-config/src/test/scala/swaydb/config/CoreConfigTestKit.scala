/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.config

import swaydb.config.compaction.PushStrategy
import swaydb.config.GenForceSave
import swaydb.testkit.TestKit.{eitherOne, randomBoolean, randomIntMax}
import swaydb.utils.OperatingSystem

import scala.util.Random

object CoreConfigTestKit {

  def randomSegmentRefCacheLife(): SegmentRefCacheLife =
    if (randomBoolean())
      SegmentRefCacheLife.Permanent
    else
      SegmentRefCacheLife.Temporary

  def randomPrefixCompressionInterval(): PrefixCompression.Interval =
    eitherOne(
      PrefixCompression.Interval.ResetCompressionAt(randomIntMax(100)),
      PrefixCompression.Interval.ResetCompressionAt(randomIntMax()),
      PrefixCompression.Interval.CompressAt(randomIntMax(100)),
      PrefixCompression.Interval.CompressAt(randomIntMax())
    )

  def randomPushStrategy(): PushStrategy =
    if (randomBoolean())
      PushStrategy.Immediately
    else
      PushStrategy.OnOverflow


  implicit class AtomicImplicits(atomic: Atomic.type) {

    def all: Seq[Atomic] =
      Seq(
        Atomic.On,
        Atomic.Off
      )

    def random: Atomic =
      if (randomBoolean())
        Atomic.On
      else
        Atomic.Off
  }

  implicit class OptimiseWritesImplicits(optimise: OptimiseWrites.type) {

    def randomAll: Seq[OptimiseWrites] =
      Seq(
        OptimiseWrites.RandomOrder,
        OptimiseWrites.SequentialOrder(initialSkipListLength = randomIntMax(100))
      )

    def random: OptimiseWrites =
      if (randomBoolean())
        OptimiseWrites.RandomOrder
      else
        OptimiseWrites.SequentialOrder(
          initialSkipListLength = randomIntMax(100)
        )
  }


  implicit class MMAPImplicits(mmap: MMAP.type) {
    def randomForSegment(): MMAP.Segment =
      if (Random.nextBoolean())
        MMAP.On(OperatingSystem.isWindows(), GenForceSave.mmap())
      else if (Random.nextBoolean())
        MMAP.ReadOnly(OperatingSystem.isWindows())
      else
        MMAP.Off(GenForceSave.standard())

    def randomForLog(): MMAP.Log =
      if (Random.nextBoolean())
        MMAP.On(OperatingSystem.isWindows(), GenForceSave.mmap())
      else
        MMAP.Off(GenForceSave.standard())
  }

}
