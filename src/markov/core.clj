(ns markov.core
  (:gen-class)
  (:require  [clojurewerkz.cassaforte.client :as client]
             [clojurewerkz.cassaforte.cql    :as cql :refer :all]
             [clojure.string :as str]
             [clojurewerkz.cassaforte.query :as q]))
           
(defn print-first [sentence-seq]
  (doseq [s sentence-seq] (println (first (str/split (:sentence_text s) #" ")))))

(defn de-quote [sentence]
  (str/replace sentence #"\"" "")) 

(defn make-word-seq [sentence]
  (re-seq #"\w+" sentence))

(defn gen-ngram 
  ([sentence n]
   (gen-ngram sentence n []))

  ([sentence n acc]
   (if-let [s (seq sentence)]
      (recur (rest sentence) n (conj acc (take n s))) 
      acc
     ))) 

(defn print-document [doc]
      (doseq [sentence doc] (println sentence)))

(defn get-corpus [corpus-name]   
   (let [session (client/connect ["127.0.0.1"] {:keyspace "markov" :protocol-version 2})]
      (println "Get Corpus : Connected to 127.0.0.1 - " corpus-name) 
      (cql/select session "corpus" (where [[= :corpus_name corpus-name]]) (allow-filtering))
     ))
  
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
      (def results (get-corpus "treasure_island"))
      (def document (map make-word-seq (map de-quote (map :sentence_text results))))
      (println (str "Result size : " (count results)))
;      (print-document document)
      (println (first document))
      (def tri-grams (gen-ngram (first document) 3))
      (print tri-grams)
     )
    
