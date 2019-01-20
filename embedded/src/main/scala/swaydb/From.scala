package swaydb

case class From[K](key: K,
                   orAfter: Boolean,
                   orBefore: Boolean,
                   before: Boolean,
                   after: Boolean)
