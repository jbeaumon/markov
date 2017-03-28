(ns markov.core
  (:gen-class)
  (:require  [clojurewerkz.cassaforte.client :as client]
             [clojurewerkz.cassaforte.cql    :as cql :refer :all]
             [clojure.string :as str]
             [clojurewerkz.cassaforte.query :as q]))
           
(defn print-first [sentence-seq]
  (doseq [s sentence-seq] (println (first (str/split (:sentence_text s) #" "))))
  )

(defn de-quote [sentence]
  (str/replace sentence #"\"" "") 
  )

(defn get-corpus [corpus-name]   
   (let [session (client/connect ["127.0.0.1"] {:keyspace "markov" :protocol-version 2})]
      (println "Get Corpus : Connected to 127.0.0.1 - " corpus-name) 
      (cql/select session "corpus" (where [[= :corpus_name corpus-name]]) (allow-filtering))
     ))
  
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
      (def results (get-corpus "treasure_island"))
      (println (str "Result size : " (count results)))
 ;     (doseq [sentence results] (println  (de-quote (:sentence_text sentence))))
     )
    

