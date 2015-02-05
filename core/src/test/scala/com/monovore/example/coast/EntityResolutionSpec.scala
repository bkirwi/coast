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
        Gen.choose(1, 20).map { n => Name(s"Item #$n") }
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

    "handle a simple example" in {

      val electrons = Category("electronics")
      val fooBar = Product(Set(Name("Foo"), Name("Bar")), 12, Set(electrons))
      val fooBaz = Product(Set(Name("Foo"), Name("Baz")), 12, Set(electrons))

      val machine = Machine.compile(graph)
        .push(Messages.from(RawProducts, Map(7 -> Seq(fooBar, fooBaz))))

      Prop.forAll(Sample.complete(machine)) { output =>

        output(AllProducts)(electrons) must_== Seq(fooBar, merge(fooBar, fooBaz))
      }
    }

    "merge all mergeable products" in {

      propNoShrink { products: Map[Int, Seq[Product]] =>

        val machine = Machine.compile(graph)
          .push(Messages.from(RawProducts, products))

        Prop.forAll(Sample.complete(machine)) { output =>

          forall(output(AllProducts)) { case (scope, values) =>

            values.tails
              .collect { case head :: tail => (head, tail) }
              .forall { case (head, tail) =>
                tail.forall { x => !matches(head, x) || merge(head, x) == x }
              }
          }
        }
      }
    }

    "not throw away any information" in {

      propNoShrink { products: Map[Int, Seq[Product]] =>

        val machine =
          Machine.compile(graph)
            .push(Messages.from(RawProducts, products))

        Prop.forAll(Sample.complete(machine)) { output =>

          val inputProducts: Seq[Product] = products.values.flatten.toSeq

          val allProducts = output(AllProducts)

          forall(inputProducts) { product =>

            product.categories forall { category =>
              allProducts(category) exists { other =>
                matches(product, other) && merge(product, other) == other
              }
            }
          }
        }
      }
    }
  }
}
