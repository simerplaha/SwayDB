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

package swaydb.data.config

import java.nio.file.Path
import scala.collection.compat._

trait DistributionDir {
  val path: Path
  val distributionRatio: Int
}

case class Dir(path: Path,
               distributionRatio: Int)

object Dir {

  implicit class DirsImplicits(dirs: Iterable[Dir]) {

    def pathsSet: Set[Path] =
      dirs.map(_.path).to(Set)

    def pathsList: Seq[Path] =
      dirs.flatMap {
        dir =>
          Seq.fill(dir.distributionRatio)(dir.path)
      }.to(Seq)
  }
}