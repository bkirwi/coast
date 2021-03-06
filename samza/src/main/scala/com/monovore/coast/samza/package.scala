package com.monovore.coast

import com.twitter.algebird.Monoid
import org.apache.samza.config.{Config, MapConfig}

import scala.collection.JavaConverters._

package object samza {

  private[samza] implicit def function1Monoid[A, B: Monoid]: Monoid[A => B] =
    Monoid.from((_: A) => Monoid.zero[B]) { (left, right) =>
      { a => Monoid.plus(left(a), right(a)) }
    }

  private[samza] implicit def function2Monoid[A, B, C: Monoid]: Monoid[(A, B) => C] =
    Monoid.from((_: A, _: B) => Monoid.zero[C]) { (left, right) =>
      { (a, b) => Monoid.plus(left(a, b), right(a, b)) }
    }
}
