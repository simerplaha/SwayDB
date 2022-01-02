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

package swaydb.core.segment.distributor

import swaydb.core.segment.Segment

import java.nio.file.Path

/**
 * Maintains the distribution of [[Segment]]s to the configured directories ratios.
 */
case class Distribution(path: Path,
                        distributionRatio: Int,
                        private[distributor] var actualSize: Int,
                        private[distributor] var expectedSize: Int) {

  def setExpectedSize(size: Int) = {
    expectedSize = size
    this
  }

  def setActualSize(size: Int) = {
    actualSize = size
    this
  }

  def missing: Int =
    expectedSize - actualSize

  def paths: Array[Path] =
    Array.fill(missing)(path)
}
