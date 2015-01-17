package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow

object EntityResolution extends ExampleMain {
  
  import coast.wire.ugly._

  type SourceID = Int
  case class Name(name: String)
  case class Category(categoryName: String)

  implicit val categoryOrdering: Ordering[Category] = Ordering.by { _.categoryName }

  case class Product(names: Set[Name], minPrice: Int, categories: Set[Category])
  
  def scope(product: Product): Seq[Category] =
    product.categories.toSeq.sorted

  def responsible(category: String, one: Product, other: Product) =
    (one.categories intersect other.categories).toSeq.sorted.headOption == Some(category)

  def matches(one: Product, other: Product): Boolean = {
    (one.names intersect other.names).nonEmpty &&
      (one.categories intersect other.categories).nonEmpty
  }

  def merge(one: Product, other: Product): Product = Product(
    names = one.names ++ other.names,
    minPrice = math.min(one.minPrice, other.minPrice),
    categories = one.categories ++ other.categories
  )

  val RawProducts = flow.Name[SourceID, Product]("raw-products")

  val AllProducts = flow.Name[Category, Product]("all-products")

  val graph = for {
    
    input <- flow.stream("input") {
      flow.source(RawProducts)
        .flatMap { e => scope(e).map { _ -> e }}
        .groupByKey
    }

    allProducts <- flow.cycle[Category, Product]("all-products-internal") { allProducts =>


      val merged = allProducts
        .aggregate(Set.empty[Product]) { (set, next) =>

          if (set contains next) set -> Seq.empty
          else {

            set.find(matches(_, next))
              .map { found =>

                val merged = merge(found, next)

                if (merged == next) (set - found + merged) -> Seq.empty
                else if (merged == found) set -> Seq.empty
                else (set - found + merged) -> Seq(merged)
              }
              .getOrElse {
                (set + next) -> Seq.empty[Product]
              }
          }
        }
        .flatMap { e => scope(e).map { _ -> e } }
        .groupByKey

        flow.merge("merged" -> merged, "raw" -> input)
    }

    _ <- flow.sink(AllProducts) { allProducts }
  } yield ()
}
