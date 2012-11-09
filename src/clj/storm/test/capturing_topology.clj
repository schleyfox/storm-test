(ns storm.test.capturing-topology
  "A capturing topology behaves similarly to complete-topology, except that
   it does not complete.  You have full control over which tuples get
   emitted from where and when."
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.testing FixedTuple])
  (:import [backtype.storm.generated GlobalStreamId Bolt])
  (:import [storm.test MultiStreamFeederSpout])
  (:require [backtype.storm.daemon [common :as common]])
  (:use [storm.test.persistent-tuple-capture-bolt])
  (:use [backtype.storm util config thrift clojure testing log]))

(defnk capturing-topology 
  [cluster-map topology :mock-sources [] :storm-conf {}]
  (let [ storm-name (str "topologytest-" (uuid))
         state (:storm-cluster-state cluster-map)
         spouts (.get_spouts topology)
         bolts (.get_bolts topology)
         feeders (reduce 
                    (fn [m k]
                      (assoc m k
                        (MultiStreamFeederSpout.)))
                    {}
                    mock-sources)
         all-streams 
          (apply concat
                 (for [[id spec] 
                       (merge (clojurify-structure spouts) 
                              (clojurify-structure bolts))]
                   (for [[stream info] (.. spec get_common get_streams)
                         :when (not (.is_direct info))]
                     (GlobalStreamId. id stream))))
         capturer (persistent-tuple-capture-bolt storm-name) ]
    (doseq [[id spout] feeders]
      (let [spout-spec (get spouts id)]
        (.set_spout_object spout-spec (serialize-component-object spout))))
    (.set_bolts topology
                (assoc (clojurify-structure bolts)
                  (uuid)
                  (let [input-spec (into {} (for [id all-streams]
                                               [id (mk-global-grouping)]))]
                    (Bolt.
                      (serialize-component-object capturer)
                      (mk-plain-component-common input-spec {} nil)))))
    (submit-local-topology 
      (:nimbus cluster-map) storm-name storm-conf topology)
    (let [storm-id (common/get-storm-id state storm-name) ]
      {:feeders feeders
       :capturer capturer
       :cluster-map cluster-map
       :name storm-name
       :storm-id storm-id})))

; (with-capturing-topology [topology-sym
;                          cluster 
;                          topology 
;                          :mock-sources [] 
;                          :storm-conf {}]
;
;   (body-exprs))
(defmacro with-capturing-topology
  [[capturing-sym cluster-map & args] & body]
  `(let [~capturing-sym (capturing-topology ~cluster-map ~@args)]
     (try
       (simulate-wait ~cluster-map)
       ~@body
       (.killTopology (:nimbus ~cluster-map) (:name ~capturing-sym))
       (while (.assignment-info (:storm-cluster-state ~cluster-map)
                                (:storm-id ~capturing-sym)
                                nil)
         (simulate-wait ~cluster-map))
     (catch Throwable t#
       (log-error t# "Error in capturing-topology"))
     (finally
       (MultiStreamFeederSpout/clear (:storm-id ~capturing-sym))))))

(defn read-current-tuples
  [capture-map & args]
  (apply read-tuples (get-results (:name capture-map)) args))

(defn feed-spout!
  ([capture-map spout-id stream-id values]
   (let [feeder ((:feeders capture-map) spout-id)]
     (.feed feeder (:storm-id capture-map) stream-id values)))
  ([capture-map spout-id values]
   (feed-spout! capture-map spout-id Utils/DEFAULT_STREAM_ID values)))

(defn wait-for-capture
  [capture-map]
  (let [storm-id (:storm-id capture-map)]
    (while (< (+ (MultiStreamFeederSpout/getNumAcked storm-id)
                 (MultiStreamFeederSpout/getNumFailed storm-id))
              (MultiStreamFeederSpout/getNumFed storm-id))
      (simulate-wait (:cluster-map capture-map)))))

(defn feed-spout-and-wait!
  [ capture-map & args ]
  (do (apply feed-spout! capture-map args)
      (wait-for-capture capture-map)))





