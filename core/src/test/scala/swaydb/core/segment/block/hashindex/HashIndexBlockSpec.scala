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

package swaydb.core.segment.block.hashindex

import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Persistent
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockConfig}
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.{SegmentBlocks, TestBase, TestCaseSweeper}
import swaydb.testkit.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class HashIndexBlockSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))

  val keyValueCount = 10000

  "searching a segment" in {
    runThis(100.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          //create perfect hash
          val compressions = if (randomBoolean()) randomCompressions() else Seq.empty

          val keyValues =
            randomizedKeyValues(
              count = 1000,
              startId = Some(1)
            )

          val segments: Slice[SegmentBlocks] =
            getBlocks(
              keyValues = keyValues,
              useCacheableReaders = randomBoolean(),
              sortedIndexConfig =
                SortedIndexBlock.Config(
                  ioStrategy = _ => randomIOStrategy(),
                  enablePrefixCompression = false,
                  shouldPrefixCompress = _ => randomBoolean(),
                  prefixCompressKeysOnly = randomBoolean(),
                  enableAccessPositionIndex = randomBoolean(),
                  optimiseForReverseIteration = randomBoolean(),
                  normaliseIndex = randomBoolean(),
                  compressions = _ => compressions
                ),
              hashIndexConfig =
                HashIndexConfig(
                  maxProbe = 1000,
                  minimumNumberOfKeys = 0,
                  minimumNumberOfHits = 0,
                  format = randomHashIndexSearchFormat(),
                  allocateSpace = _.requiredSpace * 10,
                  ioStrategy = _ => randomIOStrategy(),
                  compressions = _ => compressions
                ),
              segmentConfig =
                SegmentBlockConfig.random2(minSegmentSize = randomIntMax(1000) max 1)
            ).get

          var keyValueIndex = 0
          var segmentsIndex = 0
          var successCount = 0

          //all key-values get written to multiple segment. So iterate and search all Segment sequentially
          //until all key-values are read via HashIndex.
          while (keyValueIndex < keyValues.size) {
            val keyValue = keyValues(keyValueIndex)
            val segment = segments(segmentsIndex)

            segment.hashIndexReader shouldBe defined
            segment.hashIndexReader.get.block.hit shouldBe segment.footer.keyValueCount
            segment.hashIndexReader.get.block.miss shouldBe 0

            HashIndexBlock.search(
              key = keyValue.key,
              hashIndexReader = segment.hashIndexReader.get,
              sortedIndexReader = segment.sortedIndexReader,
              valuesReaderOrNull = segment.valuesReader.orNull
            ) match {
              case Persistent.Partial.Null =>
                //may be it's in the next Segment.
                segmentsIndex += 1

              case found: Persistent.Partial =>
                found.toPersistent shouldBe keyValue
                successCount += 1
                keyValueIndex += 1
            }
          }

          successCount shouldBe keyValues.size
      }
    }
  }
}
