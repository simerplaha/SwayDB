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

package swaydb.core.util.queue

protected sealed trait Node[+A] {
  def isEmpty: Boolean
  def previous: Node[A]
  def next: Node[A]
  def value: A
}

protected case object Node {

  final case object Empty extends Node[Nothing] {
    override val isEmpty: Boolean = true
    override val previous: Node[Nothing] = Node.Empty
    override val next: Node[Nothing] = Node.Empty
    override def value: Nothing = throw new Exception(s"${Node.productPrefix}.${Empty.productPrefix} does not have a value.")
  }

  class Value[A](val value: A,
                 @volatile var previous: Node[A],
                 @volatile var next: Node[A]) extends Node[A] {
    override def isEmpty: Boolean = false
  }

}
