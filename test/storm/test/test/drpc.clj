(ns storm.test.test.drpc
  (:use [clojure.test])
  (:use [backtype.storm testing config])
  (:use [storm.test drpc capturing-topology util])
  (:import [backtype.storm LocalDRPC])
  (:import [storm.test DRPCExclamationTopology]))

(defn drpc-exclamation-topology
  [drpc]
  (DRPCExclamationTopology/makeTopology drpc))

(deftest test-drpc
  (with-quiet-logs
    (with-simulated-time-local-cluster [cluster]
      (let [drpc (LocalDRPC.)]
        (with-capturing-topology [capture
                                  cluster
                                  (drpc-exclamation-topology drpc)
                                  :storm-conf {TOPOLOGY-DEBUG true}]
          (dotimes [i 5]
            (is (= (str "hello" i "!")
                  (execute-drpc! cluster drpc "exclamation" (str "hello" i))))))))
    )
)
