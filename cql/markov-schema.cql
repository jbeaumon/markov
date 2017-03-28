CREATE KEYSPACE IF NOT EXISTS markov WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS markov.corpus (
    sentence_id uuid,
    corpus_name text,
    sentence_text text,
PRIMARY KEY (sentence_id, corpus_name)
);

