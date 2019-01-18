package swaydb.core.function

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}

import swaydb.core.data.SwayFunction
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

trait FunctionStore {
  def get(functionId: Slice[Byte]): Option[SwayFunction]

  def put(functionId: Slice[Byte], function: SwayFunction): SwayFunction
}

object FunctionStore {

  def memory() =
    new MemoryStore()

}

class MemoryStore extends FunctionStore {

  private val functions = new ConcurrentHashMap[Slice[Byte], SwayFunction]()
  //  private val functions = new ConcurrentSkipListMap[Slice[Byte], SwayFunction](KeyOrder.default)

  override def get(functionId: Slice[Byte]): Option[SwayFunction] =
    Option(functions.get(functionId))

  override def put(functionId: Slice[Byte], function: SwayFunction): SwayFunction =
    functions.put(functionId, function)
}
