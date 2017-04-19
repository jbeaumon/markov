(ns markov.core
  (:gen-class)
  (:require  [clojurewerkz.cassaforte.client :as client]
             [clojurewerkz.cassaforte.cql    :as cql :refer :all]
             [clojure.string :as str]
             [clojurewerkz.cassaforte.query :as q]))

(defn de-quote [sentence]
  "Remove quotes from a string."
  (str/replace sentence #"\"" "")) 

(defn prep-sentence [sentence]
  "Add START and END tags to a sentence"
  (str "START " (str/replace sentence #"\"" "") " END")) 

(defn make-word-seq [sentence]
  "Split a sentence into a seq of words"
   (re-seq #"\w+" sentence))

(defn hash-ngram [ngram]
  {:root (take 2 ngram) :next-word (last ngram)}) 

(defn gen-ngram 
  "split a sentence into groups of 'n' "
  ([sentence n]
   (gen-ngram sentence n []))

  ([sentence n acc]
   (if-let [s (seq sentence)]
      (recur (rest sentence) n (conj acc (take n s))) 
      acc
     ))) 

(defn gen-trigram [sentence]
  (gen-ngram sentence 3)
  )

(defn print-document [doc]
      (doseq [sentence doc] (println sentence)))

(defn get-corpus [corpus-name]   
  "Retrieve a corpus from the database."
   (let [session (client/connect ["127.0.0.1"] {:keyspace "markov" :protocol-version 2})]
      (println "Get Corpus : Connected to 127.0.0.1 - " corpus-name) 
      (cql/select session "corpus" (where [[= :corpus_name corpus-name]]) (allow-filtering))
     ))

(defn generate-corpus-trigrams [corpus-name]
  "Turn an entire corpus into tri-grams"
  (let [corpus-results (get-corpus corpus-name)
        document (map make-word-seq (map prep-sentence (map :sentence_text corpus-results)))
        trigrams (mapcat gen-trigram document)
        ]
    
       (map hash-ngram trigrams)
    )
  )

(defn generate-sentence [hashed-trigrams]
  "Use a set of tri-grams to generate a  "
  (let [starters (filter #(= "START" (first (:root %))) hashed-trigrams)
        starter-idx (rand-int (count starters))
        sentence-pieces (filter #(not= "START" (first (:root %))) hashed-trigrams) 
        starter (nth starters starter-idx) 
        root (second (:root starter))
        sentence (seq [root (:next-word starter)])
        ]
    (loop [s sentence, prev-two sentence]
      (let [next-choices (filter #(= prev-two (:root %)) sentence-pieces)
            next-word (:next-word (nth next-choices (rand-int (count next-choices))))
            next-prev (seq [(last s) next-word])
            ]

        (if (= (last s) "END")
          (str/join " "(butlast (flatten s))) 
          (recur (seq [s next-word]) next-prev)
          )
        )
      )
    )
  )
  
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (def shakespeare  (generate-corpus-trigrams "shakespeare_complete"))
  (generate-sentence shakespeare)
     )
    
