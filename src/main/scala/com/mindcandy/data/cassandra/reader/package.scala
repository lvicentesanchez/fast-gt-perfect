package com.mindcandy.data.cassandra

package object reader {
  implicit val uniqueRowImplicitReader = new UniqueRowReader {}
}
