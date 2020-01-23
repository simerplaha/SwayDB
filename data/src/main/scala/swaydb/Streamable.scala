///*
// * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb
//
//protected trait Streamable[A] {
//
//  def foreach[U](f: A => U): Stream[Unit]
//
//  def map[B](f: A => B): Stream[B]
//  def flatMap[B](f: A => Stream[B]): Stream[B]
//
//  def drop(count: Int): Stream[A]
//  def dropWhile(f: A => Boolean): Stream[A]
//
//  def take(count: Int): Stream[A]
//  def takeWhile(f: A => Boolean): Stream[A]
//
//  def filter(f: A => Boolean): Stream[A]
//  def filterNot(f: A => Boolean): Stream[A]
//
//  def collect[B](pf: PartialFunction[A, B]): Stream[B]
//  def collectFirst[B, T[_]](pf: PartialFunction[A, B])(implicit bag: Bag[T]): T[Option[B]]
//
//  def lastOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]]
//
//  def foldLeft[B, BAG[_]](initial: B)(f: (B, A) => B)(implicit bag: Bag[BAG]): BAG[B]
//
//  def size[BAG[_]](implicit bag: Bag[BAG]): BAG[Int]
//}
