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

package swaydb.effect

import com.typesafe.scalalogging.LazyLogging
import swaydb.testkit.RunThis.eventual
import swaydb.utils.{IDGenerator, OperatingSystem}

import java.nio.file.{Path, Paths}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.DurationInt

object EffectTestSweeper extends LazyLogging {

  def apply[T](code: EffectTestSweeper => T): T = {
    val sweeper = new EffectTestSweeper {}
    val result = code(sweeper)
    sweeper.deleteAllPaths()
    result
  }

  implicit class EffectTestPathSweeperImplicits(path: Path) {
    def sweep()(implicit sweeper: EffectTestSweeper): Path =
      sweeper sweepPath path
  }
}

trait EffectTestSweeper extends LazyLogging {

  implicit lazy val idGenerator: IDGenerator = IDGenerator()

  private val testNumber = new AtomicInteger(0)

  private val paths: ConcurrentLinkedQueue[Path] = new ConcurrentLinkedQueue[Path]()

  private val projectTargetDirectory: String =
    getClass.getClassLoader.getResource("").getPath

  private val projectDirectory: Path =
    if (OperatingSystem.isWindows())
      Paths.get(projectTargetDirectory.drop(1)).getParent.getParent
    else
      Paths.get(projectTargetDirectory).getParent.getParent

  private val rootTestDirectory: Path =
    projectDirectory.resolve("TEST_DIRECTORY")

  val testDirectory: Path =
    rootTestDirectory.resolve(s"TEST-${testNumber.incrementAndGet()}")

  def sweepPath(path: Path): Path = {
    paths add path
    path
  }

  /**
   * Terminates all sweepers immediately.
   */
  def deleteAllPaths(): Unit = {
    logger.info(s"Terminating ${classOf[EffectTestSweeper].getSimpleName}")

    paths forEach {
      path =>
        eventual(10.seconds) {
          Effect.walkDelete(path)
          paths remove path
        }
    }

    eventual(10.seconds)(Effect.walkDelete(testDirectory))
  }
}
