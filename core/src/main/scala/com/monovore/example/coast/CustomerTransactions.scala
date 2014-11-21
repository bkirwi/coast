package com.monovore.example.coast

import com.monovore.coast

/**
 * Based on the discussion in this thread:
 *
 * http://mail-archives.apache.org/mod_mbox/incubator-samza-dev/201411.mbox/%3CCAFhxiSQ4V3KTt2L4CcRVHrKDRi-oS26LGCGvhSemKVPH-SW_RA@mail.gmail.com%3E
 */
object CustomerTransactions extends ExampleMain {

  import coast.wire.ugly._

  type CustomerID = String
  type TransactionID = String

  case class Customer()
  case class Transaction()

  val Customers = coast.Name[CustomerID, Customer]("customers")
  val CustomerTransactions = coast.Name[TransactionID, CustomerID]("customer-transactions")
  val Transactions = coast.Name[TransactionID, Transaction]("transactions")

  val CustomerInfo = coast.Name[CustomerID, (Customer, Seq[Transaction])]("customer-info")

  override def flow: coast.Flow[Unit] = for {

    transactionsByCustomer <- coast.label("transactions-by-customer") {

      (coast.source(Transactions).latestOption join coast.source(CustomerTransactions).latestOption)
        .stream
        .flatMap { case (latestTransaction, allCustomers) =>

          val both = for {
            transaction <- latestTransaction.toSeq
            customer <- allCustomers
          } yield customer -> transaction

          both.toSeq
        }
        .groupByKey
    }

    _ <- coast.sink(CustomerInfo) {

      val allCustomerTransactions = transactionsByCustomer.fold(Seq.empty[Transaction]) { _ :+ _ }

      val latestCustomerInfo = coast.source(Customers).latestOption

      (latestCustomerInfo join allCustomerTransactions)
        .stream
        .flatMap { case (customerOption, transactions) =>

          customerOption
            .map { _ -> transactions }
            .toSeq
        }
    }
  } yield ()
}
