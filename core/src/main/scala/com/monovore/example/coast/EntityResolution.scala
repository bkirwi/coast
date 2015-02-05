package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow.{AnyStream, Flow, Topic}

import scala.annotation.tailrec

object EntityResolution extends ExampleMain {

  import coast.wire.ugly._

  type SourceID = Int

  case class Name(name: String)

  case class Category(categoryName: String)

  implicit val categoryOrdering: Ordering[Category] = Ordering.by { _.categoryName }

  case class Product(names: Set[Name], minPrice: Int, categories: Set[Category])

  def scope(product: Product): Seq[Category] =
    product.categories.toSeq.sorted

  def matches(one: Product, other: Product): Boolean = {
    (one.names intersect other.names).nonEmpty &&
      (one.categories intersect other.categories).nonEmpty
  }

  def merge(one: Product, other: Product): Product = Product(
    names = one.names ++ other.names,
    minPrice = math.min(one.minPrice, other.minPrice),
    categories = one.categories ++ other.categories
  )

  val RawProducts = Topic[SourceID, Product]("raw-products")

  val AllProducts = Topic[Category, Product]("all-products")

  def groupByScope[A](stream: AnyStream[A, Product]) =
    stream
      .flatMap { e => scope(e).map { _ -> e } }
      .groupByKey

  val graph = for {

    allProducts <- Flow.cycle[Category, Product]("all-products-merged") { allProducts =>

      for {

        scoped <- Flow.stream("scoped-products") {

          Flow.merge(
            "all" -> groupByScope(Flow.source(RawProducts)),
            "raw" -> groupByScope(allProducts)
          )
        }
      } yield {

        scoped
          .transform(Set.empty[Product]) { (set, next) =>

            @tailrec
            def mergeAll(set: Set[Product], next: Product): (Set[Product], Option[Product]) = {

              set.find(matches(_, next)) match {
                case Some(found) => {
                  val merged = merge(found, next)

                  if (merged == found) set -> None
                  else mergeAll(set - found, merged)
                }
                case None => (set + next) -> Some(next)
              }
            }

            val (newSet, output) = mergeAll(set, next)

            newSet -> output.toSeq
          }
      }
    }

    _ <- Flow.sink(AllProducts) { allProducts }
  } yield ()
}
