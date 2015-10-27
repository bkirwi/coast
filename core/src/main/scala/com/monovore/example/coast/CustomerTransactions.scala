package com.monovore.example.coast

import com.monovore.coast
import coast.flow
import com.monovore.coast.flow.{Flow, Topic}
import com.monovore.coast.wire.Protocol

/**
 * Based on the discussion in this thread:
 *
 * http://mail-archives.apache.org/mod_mbox/incubator-samza-dev/201411.mbox/%3CCAFhxiSQ4V3KTt2L4CcRVHrKDRi-oS26LGCGvhSemKVPH-SW_RA@mail.gmail.com%3E
 */
object CustomerTransactions extends ExampleMain {

  import Protocol.native._

  type CustomerID = String
  type TransactionID = String

  case class Customer()
  case class Transaction()

  val Customers = Topic[CustomerID, Customer]("customers")
  val CustomerTransactions = Topic[TransactionID, CustomerID]("customer-transactions")
  val Transactions = Topic[TransactionID, Transaction]("transactions")

  val CustomerInfo = Topic[CustomerID, (Customer, Seq[Transaction])]("customer-info")

  override def graph: Flow[Unit] = for {

    transactionsByCustomer <- Flow.stream("transactions-by-customer") {

      (Flow.source(Transactions).latestOption join Flow.source(CustomerTransactions).latestOption)
        .updates
        .flatMap { case (latestTransaction, allCustomers) =>

          val both = for {
            transaction <- latestTransaction.toSeq
            customer <- allCustomers
          } yield customer -> transaction

          both.toSeq
        }
        .groupByKey
    }

    _ <- Flow.sink(CustomerInfo) {

      val allCustomerTransactions = transactionsByCustomer.fold(Seq.empty[Transaction]) { _ :+ _ }

      val latestCustomerInfo = Flow.source(Customers).latestOption

      (latestCustomerInfo join allCustomerTransactions)
        .updates
        .flatMap { case (customerOption, transactions) =>

          customerOption
            .map { _ -> transactions }
            .toSeq
        }
    }
  } yield ()
}
