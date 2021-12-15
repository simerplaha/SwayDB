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

import scala.annotation.unchecked.uncheckedVariance

private[swaydb] sealed trait OptionMutable[+T] extends SomeOrNoneCovariant[OptionMutable[T], OptionMutable.Some[T]] {

  override def noneC: OptionMutable[Nothing] = OptionMutable.Null
  def setValue(value: T@uncheckedVariance): Unit
  def value: T
  def toOption: Option[T] =
    this match {
      case OptionMutable.Null =>
        None

      case OptionMutable.Some(value) =>
        Some(value)
    }
}

private[swaydb] case object OptionMutable {

  final case object Null extends OptionMutable[Nothing] {
    private def throwException = throw new Exception(s"${this.productPrefix} is of type Null.")

    override def isNoneC: Boolean = true
    override def getC: Some[Nothing] = throwException
    override def setValue(value: Nothing): Unit = throwException
    override def value: Nothing = throwException
  }

  final case class Some[+T](var value: T@uncheckedVariance) extends OptionMutable[T] {

    override def isNoneC: Boolean = false
    override def getC: Some[T] = this

    def setValue(value: T@uncheckedVariance): Unit =
      this.value = value
  }
}
