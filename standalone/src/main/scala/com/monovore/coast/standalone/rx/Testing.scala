package com.monovore.coast.standalone.rx

import rx.lang.scala.{Subject, Subscriber, Observable}

sealed trait WhatThing[+A]
case class Thing(key: String, value: Long) extends WhatThing[Nothing]
case class What[A](value: A) extends WhatThing[A]

object Testing {

  def mergeAll[A](main: Observable[Thing], others: Map[String, Observable[WhatThing[A]]]): Observable[A] = {

    var stat = Map.empty[String, Long].withDefaultValue(0L)

    val subject = Subject[A]()

    val out = subject.replay(10)

    class Sub[-A] extends Subscriber[A] {
      def pull(n: Long) = request(n)
    }

    lazy val mainSub = new Sub[Thing] {
      override def onStart(): Unit = {
        pull(1L)
      }

      override def onNext(value: Thing): Unit = {
        val Thing(stream, other) = value
        if (other > stat(stream)) {
          state(stream).pull(1L)
        }
        else {
          request(1)
        }
      }
    }

    lazy val state: Map[String, Sub[WhatThing[A]]] = others.map { case (name, obs) =>
      val x = new Sub[WhatThing[A]] {
        override def onStart(): Unit = {
          request(0L)
        }

        override def onNext(value: WhatThing[A]): Unit = value match {
          case What(message) => {
            subject.onNext(message)
            request(1)
          }
          case Thing(_, n) => {
            if (n > stat(name)) {
              stat += (name -> n)
              println(stat)
              mainSub.pull(1L)
            }
            else request(1)
          }
        }
      }

      obs.subscribe(x)

      name -> x
    }

    main.subscribe(mainSub)

    subject
  }

  def main(args: Array[String]): Unit = {

    val main = Subject[Thing]()

    val stuffs = Map(
      "a" -> Observable.from(Seq(What("A"), Thing("", 10L), What("B"))),
      "b" -> Observable.from(Seq(What("C"), What("D"), Thing("", 15L), What("E"), Thing("", 25L)))
    )

    mergeAll(main, stuffs).foreach(println)

    Seq(
      Thing("a", 10L),
      Thing("b", 20L),
      Thing("a", 15L)
    ).foreach(main.onNext)
  }
}
