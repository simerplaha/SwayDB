package swaydb.slice

trait SliceROCompanion {

  implicit class ByteSliceROReader(self: SliceRO[Byte]) {
    @inline def createReader(): SliceReader =
      SliceReader(self.cut()) //TODO - remove cut
  }

}
