package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow

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

  val RawProducts = flow.Name[SourceID, Product]("raw-products")

  val AllProducts = flow.Name[Category, Product]("all-products")

  def groupByScope[A](stream: flow.Stream[A, Product]) =
    stream
      .flatMap { e => scope(e).map { _ -> e }}
      .groupByKey

  val graph = for {

    allProducts <- flow.cycle[Category, Product]("all-products-merged") { allProducts =>

      for {
        scoped <- flow.stream("scoped-products") {
          flow.merge(
            "all" -> groupByScope(flow.source(RawProducts)),
            "raw" -> groupByScope(allProducts)
          )
        }
      } yield {

        scoped
          .aggregate(Set.empty[Product]) { (set, next) =>

            @tailrec
            def doMerge(set: Set[Product], next: Product): (Set[Product], Seq[Product]) = {

              set.find(matches(_, next)) match {
                case Some(found) => {
                  val merged = merge(found, next)

                  if (merged == found) set -> Seq.empty
                  else if (merged == next) doMerge(set - found, next)
                  else doMerge(set - found, merged)
                }
                case None =>  (set + next) -> Seq(next)
              }
            }

            doMerge(set, next)
          }
      }
    }


    _ <- flow.sink(AllProducts) { allProducts }
  } yield ()
}
