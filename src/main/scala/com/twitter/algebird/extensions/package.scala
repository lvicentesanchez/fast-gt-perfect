package com.twitter.algebird

import java.nio.ByteBuffer

package object extensions {
  implicit class SpaceSaverExtensionMethods(val obj: SpaceSaver.type) extends AnyVal {
    def fromBytes(byes: Array[Byte]): SpaceSaver[String] =
      SpaceSaver(400, "")

    def fromByteBuffer(byes: ByteBuffer): SpaceSaver[String] =
      SpaceSaver(400, "")
  }
}
