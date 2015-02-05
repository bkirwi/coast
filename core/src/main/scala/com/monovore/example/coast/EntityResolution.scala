package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow.{AnyStream, Flow, Topic}

import scala.annotation.tailrec

/**
 * Based on the D-Swoosh algorithm by Stanford's Entity Resolution group.
 */
object EntityResolution extends ExampleMain {

  import coast.wire.ugly._

  // Basic entity resolution / 'swoosh' setup

  case class Name(name: String)

  case class Category(categoryName: String)

  implicit val categoryOrdering: Ordering[Category] = Ordering.by { _.categoryName }

  case class Product(names: Set[Name], minPrice: Int, categories: Set[Category])

  def matches(one: Product, other: Product): Boolean = {
    (one.names intersect other.names).nonEmpty &&
      (one.categories intersect other.categories).nonEmpty
  }

  def merge(one: Product, other: Product): Product = Product(
    names = one.names ++ other.names,
    minPrice = math.min(one.minPrice, other.minPrice),
    categories = one.categories ++ other.categories
  )

  def scope(product: Product): Seq[Category] =
    product.categories.toSeq.sorted

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

  // D-Swoosh job definition

  // New product info, arbitrarily partitioned
  val RawProducts = Topic[Int, Product]("raw-products")

  // For each category, a stream of all products in that category
  val AllProducts = Topic[Category, Product]("all-products")

  val graph = for {

    // This stream holds the results of the merge within each category
    // Since a merge in one category could trigger a merge in another,
    // we need to pass the output back in as input, so this definition is cyclic.
    // To help avoid redundant work, the Boolean tracks whether or not the product
    // is the result of a merge.
    allProducts <- Flow.cycle[Category, (Product, Boolean)]("all-products-merged") { allProducts =>

      for {

        // This stream takes both the raw products and the merge output,
        // broadcasting them to all the categories in scope
        scoped <- Flow.stream("scoped-products") {

          def groupByScope[A](stream: AnyStream[A, Product]) =
            stream
              .flatMap { e => scope(e).map { _ -> e } }
              .groupByKey

          Flow.merge(
            "all" -> groupByScope(Flow.source(RawProducts)),
            "raw" -> groupByScope(allProducts.collect { case (product, true) => product })
          )
        }

      } yield {

        scoped
          .transform(Set.empty[Product]) { (set, next) =>

            val (newSet, output) = mergeAll(set, next)

            newSet -> {
              output
                .map { product => product -> (product != next) }
                .toSeq
            }
          }
      }
    }

    // This just copies the output of the merge to the AllProducts stream
    _ <- Flow.sink(AllProducts) {
      allProducts.map { case (product, _) => product }
    }
  } yield ()
}
