(ns storm.test.persistent-tuple-capture-bolt
  (:use [backtype.storm.clojure])
  (:import [backtype.storm.task IBolt])
  (:import [backtype.storm.testing FixedTuple]))

(def ^:private emitted-tuples (atom {}))

(defn get-results [storm-name] (@emitted-tuples storm-name))

(defbolt persistent-tuple-capture-bolt [] {:params [name]}
  [tuple collector]
  (let [ component (.getSourceComponent tuple) ]
    (swap! emitted-tuples
           (fn [emitted]
             (let [ for-name (emitted name {})
                    for-component (for-name component []) ]
               (assoc 
                 emitted
                 name
                 (assoc 
                   for-name
                   component
                   (conj for-component (FixedTuple. 
                                         (.getSourceStreamId tuple)
                                         (.getValues tuple))))))))
    (ack! collector tuple)))
