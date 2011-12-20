# storm-test

This contains some potentially useful testing functions for testing storm
topologies.  It is meant to compliment those already included in storm itself.

You secretly wanted to use clojure to test your topologies anyway (even if you
haven't figured out that that is what you want yet).

## Usage

### storm.test.util

Currently this only contains a macro to tell Storm to shut up so you can see
the test output.  It is used like so:

    (with-quiet-logs
      (with-local-cluster [cluster]
        ...
      ))

### storm.test.capturing-topology

Capturing topology behaves very similarly to a complete-topology, except that
you have control over when tuples are emitted and when you read results from
the tuple capturer.  This is useful if you want to be able to check the state
of the topology in between events.  Helpers are provided to wait until all
current tuples are acked or failed.  Tuple capturing is implemented using
immutable data structures and Clojure atoms, so you could hold on to an old
version of the tuples, emit some more data, and then compare.

It is important to use this in simulated time.

Usage is like so:

    (with-simulated-time-local-cluster [cluster]
      (let [ topology (mk-your-topology) ]
        (with-capturing-topology [ capture
                                   cluster
                                   topology
                                   :mock-sources ["words"]
                                   :storm-conf {TOPOLOGY-DEBUG true} ]
          (feed-spout! capture "words" ["monday"])
          ; whee, not waiting around for anything is fun, I could feed so many
          ; tuples through right now!
          (feed-spout-and-wait! capture "words" ["serious"])
          ; Now I'm waiting for all tuples that have been fed in to get acked
          ; or failed, I'm so mature.
          (read-current-tuples capture "words")
          ; Cool, now I can see [["monday"] ["serious"]] are the tuples
          ; emitted by my spout.)))

You can also play with the clusters time and other such nasty pranks inside of
here.

## TODO

1. tracked-capturing-topology for use with tracked clusters.  I go back and
   forth on whether this would help many situations.
2. DRPC testing helpers.
3. Failure testing: attach a bolt that fails tuples under certain conditions
   as well as beef up testing spouts to replay realistically.
4. Test-generative helpers.  test-generative seems cool, would be nice to test
   topologies with a whole mess of randomly generated tuples.

## License

Copyright (C) 2011 Ben Hughes

Distributed under the Eclipse Public License, the same as Clojure.
