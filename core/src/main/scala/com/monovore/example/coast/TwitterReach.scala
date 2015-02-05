package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow.{Flow, Topic}
import java.net.URI
import scala.util.Try

/* This streaming job is loosely based on Trident's 'Twitter reach' example
 * here:
 *
 * https://storm.apache.org/documentation/Trident-tutorial.html#toc_2
 *
 * Both jobs calculate the 'reach' of a URI on Twitter, which is the total
 * number of Twitter users that have seen a particular link, by joining a user's
 * links against their set of followers. However, there are a few major
 * differences between the two:
 *
 * - The Trident job is RPC-style: it only calculates the reach for a particular
 * tweet when requested by a user. This example job continuously maintains the
 * reach count for all links.
 *
 * - The Trident job relies on external tables for the id -> followers and link
 * -> tweeted_by joins. The job here maintains that state itself, by subscribing
 * to the stream of changes.
 */

object TwitterReach extends ExampleMain {

  /* We'll start this file with some top-level definitions. If you have multiple
   * `coast` jobs in the same codebase that share data formats or streams, you'd
   * probably want to pull these out to a common file so all the jobs can access
   * them.
   *
   * First, we'll define a few `Topic`s. A `Topic` corresponds closely to a topic
   * in Kafka; it groups together the topic name (a `String`) and the types of the
   * partition keys and messages.
   *
   * Both our inputs are partitioned by user ID. Every time the user sends a
   * tweet, we get the tweet's text on the `tweets` input stream. Every time a
   * user gets a new follower, we get the follower's ID on the `followers`
   * stream. `reach` is the job's output stream. Every time a user tweets a
   * link, our job will write out the URI with the updated count on this stream.
   *
   * I'll be lazy here and use Strings for IDs. You can probably think of
   * something more appropriate.
   */

  type UserID = String 
  type FollowerID = String

  val Tweets = Topic[UserID, String]("tweets")
  val Followers = Topic[UserID, FollowerID]("followers")

  val Reach = Topic[URI, Int]("reach")

  /* `coast` uses implicit parameters to decide how to partition and serialize
   * your data. Don't be frightened! It's both safer and less verbose than using
   * configuration or a global registry, and the error messages are better.
   *
   * For now, I'm importing `flow.wire.ugly._`, which uses `Object.hashCode`
   * for partitioning and java serialization on the wire. I suggest not doing
   * this in production, but it's handy for experimenting.
   */

  import coast.wire.ugly._

  /* Now we come to the job logic. We're building up a `Flow` object here; this
   * defines the stream-processing graph and associates topic names with the
   * actual processing logic. The `for` block here may seem a bit odd, but it
   * makes it possible to track the stream names and types without cluttering
   * the job itself.
   *
   * This job is split into two stages. The first gathers up the followers that
   * saw a particular link in a particular tweet; the second accumulates all the
   * followers that have ever seen a link, writing the total count to the output
   * stream.
   */

  val graph = for {

    /* This next line defines an internal stream. The string in the middle gives
     * a name to the stream -- in the Samza backend, for example, this
     * is the name of both the output Kafka topic and the Samza job that
     * produces it. On the left, we bind the output to a variable, so we can use
     * it as an input to other stages. On the right, we're opening a block: this
     * holds the 'definition' of the stream.
     */

    followersByURI <- Flow.stream("followers-by-uri") {

      /* This little definition has two parts. The first,
       * `Flow.source(Followers)`, subscribes us to the `followers` stream we
       * defined above. This gives us a `GroupedStream[UserID, FollowerID]` --
       * it's an ongoing stream of events, and the types match up with the types
       * we defined for `Followers` above.
       *
       * The second part has the form `.fold(initial)(update function)`. Here, it
       * accumulates all the followers for a given user into a single set: the
       * return type here is `GroupedPool[UserID, Set[FollowerID]]`. A pool is like a
       * stream with a current value for each key; or, if you prefer, like a
       * table with a changelog stream. In this case, calculating the current
       * value requires keeping state; `coast` will take care of this,
       * serializing it when necessary using the formatters we defined above.
       *
       * Streams and pools are `coast`'s two main abstractions for streaming data;
       * all transformations and joins we do below result in one of these two
       * types.
       */

      val followersByUser =
        Flow.source(Followers)
          .fold(Set.empty[FollowerID]) { _ + _ }

      /* This defines a stream that extracts URIs from tweets. If a tweet
       * contains multiple URIs, we'll get multiple events in the stream.
       *
       * There's not much to say here, except to note the similarity between
       * this and the usual Scala collections API, and the poor quality of my
       * URI validation code.
       */

      val tweetedLinks =
        Flow.source(Tweets)
          .flatMap { _.split("\\s+") }
          .filter { _.startsWith("http") }
          .map { maybeURI => Try(new URI(maybeURI)).toOption }
          .flattenOption

      /* After that, we join the two together to make a new stream, then regroup
       * by key. Each of these steps could probably use a little more explanation.
       *
       * Recall that tweetedLinks is a stream, and followersByUser is a pool.
       * When we join the two, it returns the type `GroupedStream[UserID, (URI,
       * Set[FollowerID])]`. It's handy to think of the 'stream-pool join' as a
       * 'lookup' operation: every time there's a new event, we look up the
       * current state for the matching key and pair the event and state
       * together. In our case, we have both the link and all of the user's
       * followers at the time -- which is exactly what we needed.
       *
       * To get the total reach, though, we'll need to get the set of all
       * followers together on a single node -- but our data is still grouped
       * by user ID. When running on a cluster, we'll have to shuffle data
       * across the network to get the proper grouping before we can accumulate.
       * That's why this job is split in two: this first part writes the data
       * grouped by link URI, so the second part can get all the followers that
       * have ever seen a particular link in the same partition.
       *
       * We'll use the `.groupByKey` convenience method, which takes a stream of
       * key/value pairs and makes a new stream where the values are grouped
       * under the keys.
       */

      (tweetedLinks join followersByUser).groupByKey
    }

    /* In the second part of the job, we're calculating the final counts and
     * writing them to the output stream. Since this is our actual output, and
     * not just an implementation detail, we write it to the named output stream
     * we defined above. Since this stream may have any number of consumers,
     * perhaps using other languages or frameworks, `coast` is careful to keep
     * public outputs like this free of duplicates or metadata.
     */

    _ <- Flow.sink(Reach) {

      /* This last bit's pretty minimal. We use another fold to accumulate all
       * the followers in one big set. Every time we see a new follower, we
       * recalculate the size and write it to our output stream.
       */

      followersByURI
        .fold(Set.empty[FollowerID]) { _ ++ _ }
        .map { _.size }
        .updates
    }

  } yield ()

  /* And that's it! Looking back, the job logic seems dwarfed by the wall of
   * text; hope everything made it through okay.
   *
   * If you have this code checked out, you can run this code with the `dot`
   * argument to print the job in graphviz format; you'll see the two main
   * stages, plus the structure inside each stage that defines the dataflow.
   * Otherwise, if you create the Kafka topics with the right names,
   * this code is ready to run on the cluster.
   */

}
