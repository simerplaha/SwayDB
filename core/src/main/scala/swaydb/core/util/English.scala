package swaydb.core.util

private[swaydb] object English {

  def plural(count: Int, word: String): String =
    if (count == 0 || count > 1) {
      word + "s"
    } else {
      word
    }

}
