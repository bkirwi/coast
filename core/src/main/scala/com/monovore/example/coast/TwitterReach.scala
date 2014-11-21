package com.monovore.example.coast

import com.monovore.coast

object TwitterReach extends ExampleMain {

  // Two lazy decisions here: using Java serialization, and modelling IDs / URLs as strings
  // In real life, you'd want a more efficient wire format, and probably some more appropriate types

  import coast.wire.ugly._

  type UserID = String
  type FollowerID = String
  type Link = String

  // Two input streams, both partitioned by user ID
  // Every time someone tweets a URL, it appears in 'tweet-urls' under the tweeter's ID
  // Every time a user follows another user, their ID appears in 'new-followers' under the followee's ID
  val TweetURLs = coast.Name[UserID, Link]("tweet-urls")
  val Followers = coast.Name[UserID, FollowerID]("new-followers")

  // 'reach' is our output stream
  // Every time a user tweets a URL, this will write out the updated reach
  val Reach = coast.Name[Link, Int]("reach")

  val flow = for {

    // Every time a URL is tweeted, get all the followers that saw that tweet
    followerStream <- coast.label("follower-stream") {

      // Here, we roll up the set of all followers under the user's ID
      val followersByUser =
        coast.source(Followers).fold(Set.empty[FollowerID]) { _ + _ }

      // For every tweet, join it with the current set of followers
      // Then, group the stream by URL instead of user ID
      coast.source(TweetURLs)
        .join(followersByUser)
        .groupByKey
    }

    // Accumulate all the followers in a set, and write out the size in a new stream
    // In real life, you'd want something like HyperLogLog for this
    _ <- coast.sink(Reach) {
      
      followerStream
        .fold(Set.empty[FollowerID]) { _ ++ _ }
        .map { _.size }
        .stream
    }

  } yield ()
}
