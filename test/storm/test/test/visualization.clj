(ns storm.test.test.visualization
  (:use [clojure.test])
  (:use [storm.test.visualization])
  (:use [backtype.storm.clojure]))

(defbolt a-bolt ["word"] [tuple collector]
  (do
    (emit-bolt! collector [(.getString tuple 0)]
                :anchor tuple)
    (ack! collector tuple)))

(defspout a-spout ["word"]
  [conf context collector]
  (spout
    (nextTuple []
      (Thread/sleep 100)
      (emit-spout! collector ["word"]))
    (ack [id])
  ))

(deftest not-really-a-test
  (let [ topo
         (topology
           {"words" (spout-spec a-spout :p 5)
            "dirty-words" (spout-spec a-spout :p 3)
            "nice-words" (spout-spec a-spout :p 10)}
           {"counter" (bolt-spec
                        {"words" ["word"]
                         "dirty-words" ["word", "rating"]
                         "nice-words" ["word"]}
                        a-bolt
                        :p 5)
            "tourettes" (bolt-spec
                          {"dirty-words" :shuffle}
                          a-bolt)
            "filthy-tourettes" (bolt-spec
                                 {["dirty-words" "obscene"] :shuffle}
                                 a-bolt
                                 :p 2)
            "return-counts" (bolt-spec
                              {"counter" :shuffle
                               ["counter" "dirty"] :shuffle
                               ["counter" "nice"] :shuffle}
                              a-bolt)}) ]
    (println (topology-to-dot topo))))
