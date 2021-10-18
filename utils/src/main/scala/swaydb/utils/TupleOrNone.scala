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

package swaydb.utils

private[swaydb] sealed trait TupleOrNone[+L, +R] extends SomeOrNoneCovariant[TupleOrNone[L, R], TupleOrNone.Some[L, R]] {
  override def noneC: TupleOrNone[Nothing, Nothing] = TupleOrNone.None
}

private[swaydb] object TupleOrNone {
  final object None extends TupleOrNone[Nothing, Nothing] {
    override def isNoneC: Boolean = true
    override def getC: Some[Nothing, Nothing] = throw new Exception("Tuple is of type Null")
  }

  case class Some[+L, +R](left: L, right: R) extends TupleOrNone[L, R] {
    override def isNoneC: Boolean = false
    override def getC: Some[L, R] = this
  }
}
