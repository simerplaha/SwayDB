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

package swaydb.core.file.sweeper

import java.nio.file.Path

private[core] sealed trait FileSweeperItem {
  def path: Path
  def isOpen(): Boolean
}

private[core] object FileSweeperItem {

  trait Closeable extends FileSweeperItem {
    def close(): Unit
  }

  trait Deletable extends FileSweeperItem {
    def delete(): Unit
  }

}
