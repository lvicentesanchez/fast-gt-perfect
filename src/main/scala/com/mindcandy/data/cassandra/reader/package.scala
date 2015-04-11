package com.mindcandy.data.cassandra

package object reader {
  implicit val trendsRowImplicitReader = new TrendsRowReader {}
  implicit val uniqueRowImplicitReader = new UniqueRowReader {}
}
