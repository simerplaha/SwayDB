/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import java.nio.file.Path
import java.util
import java.util.concurrent.ConcurrentLinkedDeque

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.segment.Segment
import swaydb.data.config.Dir

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.compat._

private[core] case class Distribution(path: Path,
                                      distributionRatio: Int,
                                      var actualSize: Int,
                                      var expectedSize: Int) {

  def setExpectedSize(size: Int) = {
    expectedSize = size
    this
  }

  def setActualSize(size: Int) = {
    actualSize = size
    this
  }

  def missing =
    expectedSize - actualSize

  def paths =
    Array.fill(missing)(path)
}

private[core] object PathsDistributor {

  val empty =
    PathsDistributor(Seq.empty, () => Iterable.empty)

  implicit val order: Ordering[Distribution] =
    new Ordering[Distribution] {
      override def compare(x: Distribution, y: Distribution): Int =
        y.missing compareTo x.missing
    }

  def apply(dirs: Seq[Dir],
            segments: () => Iterable[Segment]): PathsDistributor =
    new PathsDistributor(dirs, segments)

  def getActualSize(dir: Dir,
                    segments: Iterable[Segment]) =
    segments.foldLeft(0) {
      case (count, segment) =>
        if (segment.path.getParent == dir.path)
          count + 1
        else
          count
    }

  def getDistributions(dirs: Seq[Dir],
                       _segments: () => Iterable[Segment]): (Array[Distribution], Int) = {
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

private[core] class PathsDistributor(var dirs: Seq[Dir],
                                     //this should be the size of
                                     segments: () => Iterable[Segment]) extends LazyLogging {

  import PathsDistributor._

  private val queue = new ConcurrentLinkedDeque[Path](distributePaths)

  @tailrec
  final def next: Path =
    if (dirs.size == 1)
      dirs.head.path
    else
      Option(queue.poll()) match {
        case Some(path) =>
          path

        case None =>
          queue.addAll(distributePaths)
          next
      }

  def distributePaths: util.List[Path] = {
    val (distributions, totalSize) = getDistributions(dirs, segments)
    val distributionResult = distribute(totalSize, distributions)
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

  def headOption =
    dirs.headOption

  def tail =
    dirs.tail

  def head =
    dirs.head

  def headPath =
    dirs.head.path

  def last =
    dirs.last

  def addPriorityPath(path: Path): PathsDistributor = {
    if (dirs.size > 1) queue.addFirst(path)
    this
  }

  def queuedPaths =
    queue.asScala

  override def toString: String =
    head.path.toString
}
