package com.monovore.example.coast

import com.monovore.coast
import coast.flow

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

  val Customers = flow.Name[CustomerID, Customer]("customers")
  val CustomerTransactions = flow.Name[TransactionID, CustomerID]("customer-transactions")
  val Transactions = flow.Name[TransactionID, Transaction]("transactions")

  val CustomerInfo = flow.Name[CustomerID, (Customer, Seq[Transaction])]("customer-info")

  override def graph: flow.FlowGraph[Unit] = for {

    transactionsByCustomer <- flow.stream("transactions-by-customer") {

      (flow.source(Transactions).latestOption join flow.source(CustomerTransactions).latestOption)
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

    _ <- flow.sink(CustomerInfo) {

      val allCustomerTransactions = transactionsByCustomer.fold(Seq.empty[Transaction]) { _ :+ _ }

      val latestCustomerInfo = flow.source(Customers).latestOption

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
