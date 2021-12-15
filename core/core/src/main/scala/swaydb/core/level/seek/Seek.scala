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

package swaydb.core.level.seek

import swaydb.core.segment.data.KeyValue

private[swaydb] object Seek {

  sealed trait Current
  object Current {
    val readStart = Read(Int.MinValue)
    case class Read(previousSegmentId: Long) extends Seek.Current
    case object Stop extends Seek.Current
    case class Stash(segmentNumber: Long, current: KeyValue) extends Seek.Current
  }

  sealed trait Next
  object Next {
    case object Read extends Seek.Next
    case object Stop extends Seek.Next
    case class Stash(next: KeyValue.Put) extends Seek.Next
  }
}
