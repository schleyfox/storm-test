(ns storm.test.drpc
  (:import [backtype.storm LocalDRPC])
  (:import [backtype.storm.utils Time])
  (:use [backtype.storm testing util]))

(defn- do-execute
  [_ client func func-args]
  (.execute client func func-args))

(defn execute-drpc! 
  "Executes a DRPC request in a possibly simulated time environment"
  [cluster-map client func func-args]
  (if (Time/isSimulating)
    (let [ req (agent nil) ]
      (send-off req do-execute client func func-args)
      (while (not (await-for 100 req))
        (advance-cluster-time cluster-map 10))
      @req)
    (do-execute client func func-args)))
