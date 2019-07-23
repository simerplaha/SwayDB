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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

protected trait Streamed[A, T[_]] {

  def foreach[U](f: A => U): Stream[Unit, T]

  def map[B](f: A => B): Stream[B, T]
  def flatMap[B](f: A => Stream[B, T]): Stream[B, T]

  def drop(count: Int): Stream[A, T]
  def dropWhile(f: A => Boolean): Stream[A, T]

  def take(count: Int): Stream[A, T]
  def takeWhile(f: A => Boolean): Stream[A, T]

  def filter(f: A => Boolean): Stream[A, T]
  def filterNot(f: A => Boolean): Stream[A, T]

  def lastOption: T[Option[A]]
  def headOption: T[Option[A]]

  def foldLeft[B](initial: B)(f: (B, A) => B): T[B]

  def size: T[Int]
}
