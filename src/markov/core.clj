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
  {:root (take 2 ngram) :next-word (last ngram)}) 

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

(defn generate-corpus-trigrams [corpus-name]
  (let [corpus-results (get-corpus corpus-name)
        document (map make-word-seq (map prep-sentence (map :sentence_text corpus-results)))
        trigrams (mapcat gen-trigram document)
        ])

       (map hash-ngram trigrams)
  )

(defn generate-sentence [hashed-trigrams]
  (let [starters (filter #(= "START" (first (:root %))) hashed-trigrams)
        starter-idx (rand-int (count starters))
        sentence-pieces (filter #(not= "START" (first (:root %))) hashed-trigrams) 
        starter (nth starters starter-idx) 
;        dummy (println starter)
        root (second (:root starter))
        sentence (seq [root (:next-word starter)])
        ]
    (loop [s sentence, prev-two sentence]
      (let [next-choices (filter #(= prev-two (:root %)) sentence-pieces)
            next-word (:next-word (nth next-choices (rand-int (count next-choices))))
            next-prev (seq [(last s) next-word])
;            dummy2 (println "Sent : " s)
;            dummy3 (println "Prev : " prev-two)
            ]

        (if (= (last s) "END")
          (str/join " "(butlast (flatten s))) 
          (recur (seq [s next-word]) next-prev)
;          (recur (conj (seq next-word) s) next-prev)
          )
        )
      )
    )
  )
  
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
      ;Pull data from DB.
      (def results (get-corpus "treasure_island"))
      ;Strip out punctuation and make each sentence a seq of words
      (def document (map make-word-seq (map prep-sentence (map :sentence_text results))))
      (println (str "Document size : " (count document)))

      ;generate trigrams ( a two word :root followed by a :next-word)
      (def trigrams (mapcat gen-trigram document))
      (def hashed-trigrams (map hash-ngram trigrams))
;      (print-document hashed-trigrams)

      ;Filter sentence starters and continuation possibilities
      (def starters (filter #(= "START" (first (:root %))) hashed-trigrams))
      (def sentence-pieces (filter #(not= "START" (first (:root %))) hashed-trigrams))
;      (print-document start)
;      (print-document sentence-pieces)
     )
    
