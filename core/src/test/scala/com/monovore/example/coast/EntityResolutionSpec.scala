package com.monovore.example.coast

import com.monovore.coast.machine.{Sample, Messages, Machine}
import org.scalacheck.{Shrink, Arbitrary, Prop, Gen}
import org.specs2.matcher.Parameters
import org.specs2.mutable._
import org.specs2.ScalaCheck

class EntityResolutionSpec extends Specification with ScalaCheck {

  implicit val scalacheck = Parameters(maxSize = 5)

  "EntityResolutionSpec" should {

    import EntityResolution._

    val products = for {
      names <- Gen.nonEmptyContainerOf[Set, Name](
        Gen.choose(1, 20).map { n => Name(s"Product #$n") }
      )
      minPrice <- Gen.choose(5, 25)
      category <- Gen.nonEmptyContainerOf[Set, Category](
        Gen.oneOf("electronics", "spice", "hardware", "candy", "boxes", "shoes").map(Category)
      )
    } yield Product(names, minPrice, category)

    implicit val arbProduct = Arbitrary(products)

    implicit val shrinkProduct = Shrink[Product] { case Product(names, price, categories) =>

      Shrink.shrink(names).map { Product(_, price, categories) } append
        Shrink.shrink(categories).map { Product(names, price, _) }
    }

    "include all the input data" in {

      prop { products: Map[SourceID, Seq[Product]] =>

        val machine = Machine.compile(graph)
          .push(Messages.from(RawProducts, products))

        Prop.forAll(Sample.complete(machine)) { output =>

          forall(output(AllProducts)) { case (scope, values) =>

            values.toList.tails.forall {
              case Nil => true
              case head :: tail => {

                tail.forall(!matches(_, head)) ||
                  values.exists { other => matches(head, other) && merge(head, other) == other }
              }
            }
          }
        }
      }
    }
  }
}
