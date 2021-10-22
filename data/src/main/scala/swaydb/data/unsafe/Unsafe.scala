package swaydb.data.unsafe

import sun.misc.Unsafe

private[swaydb] object Unsafe {

  val get: Unsafe = {
    classOf[Unsafe]
      .getDeclaredFields
      .find {
        field =>
          field.getType == classOf[Unsafe]
      }
      .map {
        field =>
          field.setAccessible(true)
          field.get(null).asInstanceOf[Unsafe]
      }
      .getOrElse(throw new IllegalStateException(s"Unable to find ${classOf[Unsafe].getName}"))
  }

}
