(ns markov.core
  (:gen-class)
  (:require  [clojurewerkz.cassaforte.client :as client]
             [clojurewerkz.cassaforte.cql    :as cql :refer :all]
             [clojure.string :as str]
             [clojure.math.numeric-tower :as math]
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

(defn hash-ngram [n ngram]
  {:n n :root (take (dec n) ngram) :next-word (last ngram)}) 

(defn hash-trigram [ngram]
  (hash-ngram 3 ngram))

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
    
       (map hash-trigram trigrams)
    )
  )

(defn weight-transitions [next-choices]
  (let [
        unique-transitions (distinct next-choices)
        ]
      (loop [u unique-transitions, w ()]
        (let [
              current-trans (first u)
              transition-ct (count (filter #(= (:next-word current-trans) (:next-word  %)) next-choices))
              ; create a record that includes the number of occurrences of the transition and a weghted float.
              ; uses the square root of the total count to add realistic weights without cutting the sentences too short.
              weighted-transition {:root (:root current-trans) :next-word (:next-word current-trans) 
                                   :num-occurrences transition-ct :weight (* (math/sqrt transition-ct) (rand)) :n (:n current-trans)}
              ]
          (if (empty? u)
            w
            (recur (rest u) (conj w weighted-transition))
            )
          )
        )
    )
  )



(defn generate-sentence [hashed-trigrams]
  "Use a set of tri-grams to generate a  "
  (let [;Filter sentence starting ngrams
        starters (filter #(= "START" (first (:root %))) hashed-trigrams)
        starter-idx (rand-int (count starters))
        ;Fileter sentence continuations
        sentence-pieces (filter #(not= "START" (first (:root %))) hashed-trigrams) 
        ;choose a random starter
        starter (nth starters starter-idx) 
        ;take the word after the START tag
        root (second (:root starter))
        ;take the  word following the root to begin the sentence
        sentence (seq [root (:next-word starter)])
        ]
    (loop [s sentence, prev-two sentence]
      (let [next-choices (filter #(= prev-two (:root %)) sentence-pieces)
;            dummy (println "NC : " next-choices)
            weighted-choices (weight-transitions next-choices)
            sorted-choices (sort-by :weight weighted-choices)
            selected-choice (last sorted-choices)
            ;next-word (:next-word (nth next-choices (rand-int (count next-choices))))
            next-word (:next-word selected-choice)
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
  (println (generate-sentence shakespeare))
     )
    
