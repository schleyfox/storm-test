(ns storm.test.test.capturing-topology
  (:use [clojure.test])
  (:use [storm.test capturing-topology util])
  (:require [backtype.storm [thrift :as thrift]])
  (:use [backtype.storm clojure testing config bootstrap log])
  (:use [backtype.storm.daemon common]))

(bootstrap)

(defbolt exclaim-bolt ["word"] [tuple collector]
  (do 
    (emit-bolt! collector [(str (.getString tuple 0) "!")]
                :anchor tuple)
    (ack! collector tuple)))

(defspout word-spout ["word"]
  [conf context collector]
  (spout
    (nextTuple []
      (Thread/sleep 100)
      (emit-spout! collector ["word"]))
    (ack [id]
      )))

(deftest test-capturing-topology
  (with-quiet-logs
    (with-simulated-time-local-cluster [cluster]
      (let [ topo (topology
                    {"words" (spout-spec word-spout)}
                    {"exclaim" (bolt-spec {"words" :shuffle}
                                          exclaim-bolt)}) ]
        (with-capturing-topology [ capture
                                   cluster
                                   topo
                                   :mock-sources ["words"]
                                   :storm-conf {TOPOLOGY-DEBUG true} ]
          (feed-spout-and-wait! capture "words" ["foo"])
          (is (ms= [["foo!"]] 
                   (read-current-tuples capture "exclaim")))
          (feed-spout-and-wait! capture "words" ["bar"])
          (is (ms= [["foo!"] ["bar!"]]
                   (read-current-tuples capture "exclaim")))
          
                                 
          )))))
