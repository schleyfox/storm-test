(ns storm.test.visualization
  "You always wanted to visualize your topology using dot.  Now you can"
  (:import [backtype.storm.generated Bolt SpoutSpec Grouping])
  (:import [java.io File])
  (:use [backtype.storm thrift util])
  (:require [clojure.string :as str])
  (:require [clojure.java [shell :as sh]
                          [browse :as browse]]))

(def ^{:private true
       :doc "Red yellow green red blue blue blue
             red purple green yellow orange red red
             red yellow green red blue blue blue
             red purple green yellow orange red red
             
             Blend them up and what do you get?
             Cerise chartrous and aqua
             mauve beige and ultramarine and every color in between
             hazo ka li ka no cha lum bum

             Color has it's harmony and just like I have said"}
  colors
  ["red" "yellow" "green" "blue" "purple" "orange" "brown" "gray" "cyan"
   "magenta" "violet" "pink" "goldenrod" "lawngreen" "cadetblue" "deeppink"
   "darkolivegreen"])

(defn- get-color
  "returns a color based on the hashcode of object"
  [obj]
  (colors (mod (.hashCode obj) (count colors))))

(defn- dot
  "Takes a list of strings and merges them into a list"
  [ & lines]
  (apply str (interpose "\n" (flatten lines))))

(defn- dot-digraph
  "Makes a digraph named 'name out of the input lines"
  [name & lines]
  (dot 
    (str "digraph " name " {")
    lines
    "}"))

(defn- nice-class-name
  [obj]
  (str/replace (type obj) (re-pattern "^.*\\.") ""))

(defn- label-node
  "labels are id (class) parallelism"
  [obj]
  (let [ obj-type (nice-class-name 
                    (deserialized-component-object
                      (condp = (type (val obj))
                        Bolt (.get_bolt_object (val obj))
                        SpoutSpec (.get_spout_object (val obj)))))
         p-hint (.. (val obj) get_common get_parallelism_hint)
         p (if (= 0 p-hint) 1 p-hint) ]
    (str "label=\"" (key obj) " (" obj-type ") p=" p "\"")))


(defn- label-spouts
  "spouts are squares"
  [spouts]
  (map (fn [spout]
         (str "  \"" (key spout) "\" "
              "["
              (label-node spout)
              ",shape=box"
              "];"))
       spouts))

(defn- label-bolts
  [bolts]
  (map (fn [bolt]
         (str "  \"" (key bolt) "\" "
              "[" (label-node bolt) "];"))
       bolts))

(defn- escape-quotes
  [s]
  (str/escape s {\\ "\\\\", \" "\\\""}))


(defn- label-connection
  [stream-id grouping]
  (let [ grouping-str (case (.getFieldName (.getSetField grouping))
                        "fields" (str (.get_fields grouping))
                        (str (.getFieldName (.getSetField grouping))))
         stream-name (if (not= stream-id "default") 
                       (str " \\\"" stream-id "\\\"")) ]
    (str "label=\""(escape-quotes grouping-str) stream-name "\"")))


(defn- draw-connections
  [bolts]
  (map
    (fn [bolt-spec]
      (let [ id (key bolt-spec)
             bolt (val bolt-spec)
             common (.get_common bolt)
             inputs (clojurify-structure (.get_inputs common)) ]
        (map
          (fn [input]
            (let [ from (key input)
                   grouping (val input)
                   from-id (.get_componentId from)
                   stream-id (.get_streamId from) ]
              (str "  \"" from-id "\" -> \"" id "\" "
                   "["
                   (if (not= stream-id "default") 
                     (str "color=" (get-color stream-id) ","))
                   (label-connection stream-id grouping)
                   "];")))
          inputs)))
    bolts))
                   


(defn topology-to-dot
  "Takes a topology and converts it to a dot digraph"
  [topology]
  (let [ spouts (clojurify-structure (.get_spouts topology))
         bolts (clojurify-structure (.get_bolts topology)) ]
    (dot-digraph "topology"
      (label-spouts spouts)
      (label-bolts bolts)
      (draw-connections bolts)
    )
  ))

(defn dot-to-png
  ([dot-str file]
   (let [ file-name (.toString file) 
          png-file-name (str file-name ".png")]
     (spit file dot-str)
     (let [ output (sh/sh "dot" "-Tpng" "-o" png-file-name file-name) ]
       (if (not= 0 (:exit output))
         (throw (RuntimeException. (:err output)))
         png-file-name))))
  ([dot-str]
   (dot-to-png dot-str (File/createTempFile "-fs-" ".dot"))))

(defn visualize-topology
  [topology]
  (browse/browse-url
    (str "file://" (dot-to-png
      (topology-to-dot
        topology)))))


