package swaydb.core.util

import scala.annotation.tailrec
import scala.reflect.ClassTag

object LimitHashMap {
  def apply[K, V: ClassTag](maxSize: Int) =
    new LimitHashMap[K, V](new Array[(K, V)](maxSize))
}

class LimitHashMap[K, V](array: Array[(K, V)]) {

  val arrayLength = array.length
  val maxProbe = 2

  def put(key: K, value: V): Unit = {
    val index = Math.abs(key.##) % arrayLength
    put(key, value, index, index, 0)
  }

  @tailrec
  private def put(key: K, value: V, hashIndex: Int, targetIndex: Int, probe: Int): Unit =
    if (probe == maxProbe) {
      array(hashIndex) = (key, value)
    } else {
      val existing = array(targetIndex)
      if (existing == null || existing._1 == key)
        array(targetIndex) = (key, value)
      else
        put(key, value, hashIndex, if (targetIndex + 1 >= arrayLength) 0 else targetIndex + 1, probe + 1)
    }

  def get(key: K): Option[V] = {
    val index = Math.abs(key.##) % arrayLength
    get(key, index, 0)
  }

  @tailrec
  private def get(key: K, index: Int, probe: Int): Option[V] =
    if (probe == maxProbe) {
      None
    } else {
      val keyValue = array(index)
      if (keyValue._1 == key)
        Some(keyValue._2)
      else
        get(key, if (index + 1 >= arrayLength) 0 else index + 1, probe + 1)
    }

  def foreach[U](value: ((K, V)) => U): Unit =
    array.foreach(value)

  def mkString(sep: String) =
    array.mkString(sep)

  override def toString: String =
    mkString(", ")
}
