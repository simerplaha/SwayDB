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

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.effect.{Dir, Reserve}
import swaydb.utils.Options

import java.nio.file.Path
import java.util
import java.util.concurrent.ConcurrentLinkedDeque
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object PathDistributor {

  private implicit val order: Ordering[Distribution] =
    new Ordering[Distribution] {
      override def compare(x: Distribution, y: Distribution): Int =
        y.missing compareTo x.missing
    }

  def apply(dirs: Seq[Dir],
            segments: () => Iterable[Distributable]): PathDistributor =
    new PathDistributor(dirs, segments)

  def getActualSize(dir: Dir,
                    segments: Iterable[Distributable]) =
    segments.foldLeft(0) {
      case (count, segment) =>
        if (segment.path.getParent == dir.path)
          count + 1
        else
          count
    }

  def getDistributions(dirs: Seq[Dir],
                       _segments: () => Iterable[Distributable]): (Array[Distribution], Int) = {
    var totalSize = 0
    lazy val segments = _segments() //lazy val ???
    val distributions: Array[Distribution] =
      dirs.map({
        dir =>
          val actualSize = getActualSize(dir, segments)
          totalSize += actualSize
          Distribution(
            path = dir.path,
            distributionRatio = dir.distributionRatio,
            actualSize = actualSize,
            expectedSize = 0
          )
      }).to(Array)

    (distributions, totalSize)
  }

  /**
   * Given the current size of the Level returns the distributions expected.
   */
  def distribute(size: Int,
                 distributions: Array[Distribution])(implicit order: Ordering[Distribution] = order): Array[Distribution] = {

    @tailrec
    def doDistribute(size: Int,
                     currentDistribution: Int): Array[Distribution] =
      IO(distributions(currentDistribution)).toOption match {
        case Some(distribution) =>
          val (expectedSize, remainingSize) =
            if (size > distribution.distributionRatio)
              (distribution.expectedSize + distribution.distributionRatio, size - distribution.distributionRatio)
            else
              (distribution.expectedSize + size, 0)

          distribution setExpectedSize expectedSize
          doDistribute(remainingSize, currentDistribution + 1)

        case None if size > 0 =>
          doDistribute(size, 0)

        case None =>
          distributions
      }

    doDistribute(size, 0).sorted
  }
}

private[core] class PathDistributor(val dirs: Seq[Dir],
                                    //this should be the size of
                                    segments: () => Iterable[Distributable]) extends LazyLogging {

  private val queue = new ConcurrentLinkedDeque[Path](distributePaths())

  //disallows concurrently performing distribution
  private val distributionReserve = Reserve.free[Unit](s"Distributing paths reserve. Directories: ${dirs.size}")

  @tailrec
  final def next(): Path =
    if (dirs.size == 1)
      dirs.head.path
    else
      Option(queue.poll()) match {
        case Some(path) =>
          path

        case None =>
          if (distributionReserve.compareAndSet(Options.unit)) {
            try
              queue.addAll(distributePaths())
            finally
              distributionReserve.setFree()

            next()
          } else {
            distributionReserve.blockUntilFree()
            next()
          }
      }

  private def distributePaths(): util.List[Path] = {
    val (distributions, totalSize) = PathDistributor.getDistributions(dirs, segments)
    val distributionResult = PathDistributor.distribute(totalSize, distributions)
    val paths: Seq[Path] =
      distributionResult flatMap {
        dist: Distribution =>
          Seq.fill(dist.missing)(dist.path)
      }

    if (paths.nonEmpty) {
      //      logger.trace(s"{} Un-even distribution. Prioritizing paths {}", head.path, paths.mkString(", "))
      paths.asJava
    }
    else //if there are no un-even distributions go back to default distribution ratio.
      dirs.flatMap(dir => Array.fill(dir.distributionRatio)(dir.path)).asJava
  }

  def headOption: Option[Dir] =
    dirs.headOption

  def tail: Seq[Dir] =
    dirs.tail

  def head: Dir =
    dirs.head

  def headPath: Path =
    dirs.head.path

  def last: Dir =
    dirs.last

  def addPriorityPath(path: Path): PathDistributor = {
    if (dirs.size > 1) queue.addFirst(path)
    this
  }

  def queuedPaths: Iterable[Path] =
    queue.asScala

  override def toString: String =
    head.path.toString
}
