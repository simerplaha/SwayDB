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

package swaydb.core.level

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

/**
 * For generating unique path ids for in-memory instances.
 */
object LevelPathGenerator {

  private val memoryPathId = new AtomicInteger(0)

  def nextMemoryPath() = Paths.get(s"MEMORY-${memoryPathId.incrementAndGet()}")

}
