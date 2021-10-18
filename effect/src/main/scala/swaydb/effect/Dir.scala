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

    @inline final def pathsSet: Set[Path] =
      dirs.map(_.path).to(Set)

    @inline final def pathsList: Seq[Path] =
      dirs.flatMap {
        dir =>
          Seq.fill(dir.distributionRatio)(dir.path)
      }.to(Seq)
  }
}
