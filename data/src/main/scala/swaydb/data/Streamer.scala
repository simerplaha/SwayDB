/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data

private[swaydb] trait Streamer[A, W[_]] {

  def map[B](f: A => B): Stream[B, W]

  def flatMap[B](f: A => Stream[B, W]): Stream[B, W]

  def drop(count: Int): Stream[A, W]

  def dropWhile(f: A => Boolean): Stream[A, W]

  def take(count: Int): Stream[A, W]

  def takeWhile(f: A => Boolean): Stream[A, W]

  def foreach[U](f: A => U): Stream[Unit, W]

  def filter(f: A => Boolean): Stream[A, W]

  def filterNot(f: A => Boolean): Stream[A, W]

  def foldLeft[B](initial: B)(f: (B, A) => B): W[B]

  def size: W[Int]

  def lastOption: W[Option[A]]

  def headOption: W[Option[A]]

}
