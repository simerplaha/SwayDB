package swaydb.core.util

import scala.annotation.tailrec

/**
 * A fixed size HashMap that inserts newer key-values to empty spaces or
 * overwrites older key-values if the space is occupied by an older
 * key-value.
 */
sealed trait LimitHashMap[K, V] extends Iterable[(K, V)] {
  def limit: Int
  def put(key: K, value: V): Unit
  def get(key: K): Option[V]
}

object LimitHashMap {

  /**
   * A Limit HashMap that tries to insert newer key-values to empty slots
   * or else overwrites older key-values if no slots are free.
   *
   * @param limit    Max number of key-values
   * @param maxProbe Number of re-tries on hash collision.
   */
  def apply[K, V](limit: Int,
                  maxProbe: Int): LimitHashMap[K, V] = {
    if (limit <= 0)
      new Empty[K, V]
    else if (maxProbe <= 0)
      new NoProbe[K, V](
        array = new Array[(K, V)](limit)
      )
    else
      new Probed[K, V](
        array = new Array[(K, V)](limit),
        maxProbe = maxProbe
      )
  }

  /**
   * @param limit Max number of key-values
   */
  def apply[K, V](limit: Int): LimitHashMap[K, V] =
    if (limit <= 0)
      new Empty[K, V]
    else
      new NoProbe[K, V](
        array = new Array[(K, V)](limit)
      )

  private class Probed[K, V](array: Array[(K, V)], maxProbe: Int) extends LimitHashMap[K, V] {

    val limit = array.length

    def put(key: K, value: V): Unit = {
      val index = Math.abs(key.##) % limit
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
          put(key, value, hashIndex, if (targetIndex + 1 >= limit) 0 else targetIndex + 1, probe + 1)
      }

    def get(key: K): Option[V] = {
      val index = Math.abs(key.##) % limit
      get(key, index, 0)
    }

    @tailrec
    private def get(key: K, index: Int, probe: Int): Option[V] =
      if (probe == maxProbe) {
        None
      } else {
        val keyValue = array(index)
        if (keyValue != null && keyValue._1 == key)
          Some(keyValue._2)
        else
          get(key, if (index + 1 >= limit) 0 else index + 1, probe + 1)
      }

    override def iterator: Iterator[(K, V)] =
      array.iterator
  }

  private class NoProbe[K, V](array: Array[(K, V)]) extends LimitHashMap[K, V] {

    val limit = array.length

    def put(key: K, value: V) =
      array(Math.abs(key.##) % limit) = (key, value)

    def get(key: K): Option[V] = {
      val value = array(Math.abs(key.##) % limit)
      if (value != null && value._1 == key)
        Some(value._2)
      else
        None
    }

    override def iterator: Iterator[(K, V)] =
      array.iterator
  }

  private class Empty[K, V] extends LimitHashMap[K, V] {
    override def limit: Int = 0
    override def put(key: K, value: V): Unit = ()
    override def get(key: K): Option[V] = None
    override def iterator: Iterator[(K, V)] = Iterator.empty
  }
}
