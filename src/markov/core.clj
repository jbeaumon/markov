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

(defn prep-sentence [sentence]
  (str "START " (str/replace sentence #"\"" "") " END")) 

(defn make-word-seq [sentence]
   (re-seq #"\w+" sentence))

(defn hash-ngram [ngram]
  {:root (first ngram) :words (rest ngram)}) 

(defn make-ngram-hash [ngrams]
  (let [])
  )

(defn gen-trigram [sentence]
  (gen-ngram sentence 3)
  )

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
      ;(def document (map make-word-seq (map de-quote (map :sentence_text results))))
      (def document (map make-word-seq (map prep-sentence (map :sentence_text results))))
      (println (str "Result size : " (count results)))
      (println (str "Document size : " (count document)))
      (println (first document))
      (def tri-grams (gen-ngram (first document) 3))
      (def trigrams (map gen-trigram document))
      (println (str "Trigram size : " (count trigrams)))
      (def hashed-ngrams (map hash-ngram trigrams))
      (println (str "Hashed-ngram size : " (count hashed-ngrams)))
      (println (last hashed-ngrams))
;      (println hashed-ngrams)

;      (def start (filter #(= "START" (:root %)) hashed-ngrams))
;      (println start)
     )
    
