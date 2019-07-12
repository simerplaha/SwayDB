package swaydb.data.config

trait BlockInfo {
  def isCompressed: Boolean
  def compressedSize: Int
  def decompressedSize: Int
}
