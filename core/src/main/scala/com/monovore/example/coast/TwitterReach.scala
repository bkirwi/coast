package com.monovore.example.coast

import java.net.URI

import com.google.common.base.Charsets
import com.google.common.hash.HashCode
import com.monovore.coast
import com.monovore.coast.wire.{Partitioner, BinaryFormat}

import scala.util.Try

/**
 * This streaming job is loosely based on Trident's 'Twitter reach' example here:
 *
 * https://storm.apache.org/documentation/Trident-tutorial.html#toc_2
 *
 * Both jobs calculate the 'reach' of a URI on Twitter, which is the total number
 * of Twitter users that have seen a particular link, by joining
 * a user's links against their set of followers. However, there are a few
 * major differences between the two:
 *
 * - The Trident job is RPC-style: it only calculates the reach for a particular
 *   tweet when requested by a user. This example job continuously
 *   maintains the reach count for all links.
 *
 * - The Trident job relies on external tables for the id -> followers and
 *   link -> tweeted_by joins. The job here maintains that state itself, by
 *   subscribing to the stream of changes.
 */
object TwitterReach extends ExampleMain {

  /**
   * We'll start this file with some top-level definitions. If you
   * have multiple `coast` jobs in the same codebase that share data formats or
   * streams, you'd probably want to pull these out to a common file so all the
   * jobs can access them.
   *
   * First, we'll define a few `Name`s. A `Name` corresponds roughly to a Kafka
   * topic; it groups together the topic name (a `String`) and the types of the
   * keys and messages.
   *
   * Both our inputs are partitioned by user ID. Every time the user sends a
   * tweet, we get the tweet's text on the `tweets` input stream. Every time
   * a user gets a new follower, we get the follower's ID on the `followers`
   * stream.
   *
   * I'll be lazy here and use Strings for IDs. You can probably think of
   * something more appropriate.
   */
  type UserID = String
  type FollowerID = String

  val Tweets = coast.Name[UserID, String]("tweets")
  val Followers = coast.Name[UserID, FollowerID]("followers")

  /**
   * This job has a single output stream, `reach`. Every time a user tweets
   * a link, our job will write out the URI with the updated count on this
   * stream.
   *
   * While this is the output stream for this particular job, it might be the
   * input stream for a downstream job... someone might want to maintain a list
   * of the all-time most popular links, for example.
   */
  val Reach = coast.Name[URI, Int]("reach")

  /**
   * `coast` uses implicit parameters to decide how to partition and serialize
   * your data. Don't be frightened! It's both safer and less verbose than using
   * configuration or a global registry, and the error messages are pretty good.
   *
   * For now, I'm importing `coast.wire.ugly._`, which uses `Object.hashCode` for
   * partitioning and java serialization on the wire. I suggest not doing this
   * in production, but it's handy for development.
   */
  import coast.wire.pretty._

  implicit val followerSetFormat =
    coast.wire.javaSerialization.formatFor[Set[FollowerID]]

  implicit val uriBinaryFormat = new BinaryFormat[URI] {

    override def write(value: URI): Array[Byte] = value.toString.getBytes(Charsets.UTF_8)

    override def read(bytes: Array[Byte]): URI = new URI(new String(bytes, Charsets.UTF_8))
  }

  implicit val uriPartitioner = new Partitioner[URI] {
    override def hash(a: URI): HashCode = HashCode.fromInt(a.hashCode())
  }

  /**
   * Now, we come to the job logic. We're building up a `Flow` object here; this
   * builds up the stream-processing DAG and associates stream names with the
   * actual processing logic. The `for` block here may seem a bit odd, but it
   * makes it possible to track the stream names and types without cluttering
   * the job logic.
   *
   * Here, we have a two-stage job. The first gathers up the followers that saw a
   * particular link in a particular tweet; the second accumulates all the followers
   * that have ever seen a link, and writes the count.
   */
  val flow = for {

    /**
     * This defines a stream with the label `followers-by-uri`. A labelled stream
     * is a lot like a named stream, but the semantics are a bit different:
     * while a named stream is part of the public API, a labelled stream is internal
     * to a single job. That's why we define it inline this way instead of
     * naming it at the top-level.
     */
    followersByURI <- coast.label("followers-by-uri") {

      /**
       * This little definition has two parts. The first, `coast.source(Followers)`,
       * subscribes us to the `followers` stream we defined above. This gives us a
       * `Stream[UserID, FollowerID]` -- it's an ongoing stream of events, and the types
       * match up with the types we defined for `Followers` above.
       *
       * The second part has the form .fold(initial)(update function)`. This
       * accumulates all the followers for a given user into a single set. The return
       * type here is `Pool[UserID, Set[FollowerID]]`. A `Pool` is like a stream with a
       * current value for each key; or, if you prefer, like a table with a changelog
       * stream. In this case, calculating the current value requires keeping state;
       * `coast` will take care of this, serializing it when necessary using the
       * formatters we defined above.
       */
      val followersByUser: coast.Pool[UserID, Set[FollowerID]] =
        coast.source(Followers).fold(Set.empty[FollowerID]) { _ + _ }

      /**
       * This defines a stream that extracts URIs from tweets. If a tweet contains multiple
       * URIs, we'll get multiple events in the stream.
       *
       * There's not much to say here, except to note the similarity between this and
       * the usual Scala collections API.
       */
      val tweetedLinks = coast.source(Tweets)
        .flatMap { _.split("\\s+") }
        .filter { _.startsWith("http") }
        .flatMap { maybeURI => Try(new URI(maybeURI)).toOption.toSeq }

      /**
       * After that, we join the two together to make a new stream and regroup by key.
       * Each stage could probably use a little more explanation.
       *
       * Recall that tweetedLinks is a stream, and followersByUser is a pool. When we
       * join the two, it returns the type Stream[UserID, (URI, Set[FollowerID)]. It's
       * handy to think of the 'stream-pool join' as a 'lookup' operation: every time
       * there's a new event, we look up the current state for the matching key and pair
       * the event and state together. In our case, we have both the link and all of the
       * user's followers at the time -- which is exactly what we needed.
       *
       * To get the total reach, though, we'll need to get the set of all followers
       * together on a single node -- but everything's still grouped by user ID. When
       * running on a cluster, we'll have to shuffle data across the network to get the
       * proper grouping before we can accumulate. That's why this job is split in two:
       * this first part writes the data grouped by link URI, so the second part can get
       * all the followers that have ever seen a particular link in the same partition.
       *
       * We'll use the `.groupByKey` convenience method, which takes a stream of
       * key/value pairs and makes a new stream where the values are grouped under the
       * keys.
       */
      (tweetedLinks join followersByUser).groupByKey
    }

    /**
     * In the second part of the job, we're calculating the final counts and writing them
     * to the output stream. To write to a named stream and not a label, we use `coast.sink`.
     */
    _ <- coast.sink(Reach) {

      /**
       * This last bit's pretty minimal: we just accumulate all the followers in one big set.
       * Every time that set's updated, we calculate the size and write it to our output stream.
       */
      followersByURI
        .fold(Set.empty[FollowerID]) { _ ++ _ }
        .map { _.size }
        .updateStream
    }

  } yield ()

  /**
   * And that's it! Looking back, the job logic seems dwarfed by the wall of text; hope everything
   * made it through okay.
   *
   * If you have this code checked out, you can run this code with the `dot` argument to
   * print the job in graphviz format; you'll see the two main stages, plus the structure inside
   * each stage that defines the dataflow.
   */
}
