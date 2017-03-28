#!/usr/local/bin/python
import re
import sys
import time
import uuid
import datetime
#import cassandra
from optparse import OptionParser
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from nltk.tokenize import sent_tokenize

LOCALHOST='127.0.0.1'

MARKOV_KEYSPACE='markov'
MINI_NUM_RECS=1550
CORPUS_UUID = uuid.uuid4()
START_TAG = '*** START OF THIS PROJECT GUTENBERG EBOOK'
END_TAG = '*** END OF THIS PROJECT GUTENBERG EBOOK'
ACCEPT_INPUT = False

def log(message, component):
    print '[LOG : (' + str(component) + ')] : ' + str(message)

def parse_args():
    descr = """Connect to a Cassandra instance and copy profile media to the media keyspace"""
    usage_msg = './xfer_media.py -u <cassandra_url>'

    parser = OptionParser(usage = usage_msg,
        description = descr,
        add_help_option = True)
    parser.add_option('-u', '--cassandra-url',
        dest = 'cassandra_url',
        help = """Url cassandra will attempt to connect to.""")
    parser.add_option('-f', '--load-file',
        dest = 'load_file',
        help = """File containing text to parse and insert to the markov.corpus table.""")
    parser.add_option('-c', '--corpus',
        dest = 'corpus',
        help = """Corpus name. """)
    parser.add_option('-d', '--dry-run',
        dest = 'dry_run',
        action="store_true",
        help = """Perform a dry run where logging is shown, but no commits are made.""")
    parser.add_option('-m', '--mini-run',
        dest = 'mini_run',
        action="store_true",
        help = """Perform a small run where only the first 100 records are processed.""")

    (options, args) = parser.parse_args()
    if not options.cassandra_url:
        log('No cassandra url provided. Defaulting to 127.0.0.1', 'parse_args')
        options.cassandra_url = '127.0.0.1'
    if not options.load_file:
        log('No input file provided. Exiting.', 'parse_args')
        sys.exit()
    if not options.corpus:
        log('No corpus provided. Exiting.', 'parse_args')
        sys.exit()
    if options.dry_run:
        log('DRY RUN in effect. No data will be transfered.', 'parse_args')
    return options


def check_start_finish(sentence):
    global ACCEPT_INPUT
    if sentence.find(START_TAG) >= 0:
        log('START tag found. Begin accpeting input.', 'check_start_finish')
        ACCEPT_INPUT = True 
    elif sentence.find(END_TAG) >= 0:
        log('END tag found. Stopping reading input.', 'check_start_finish')
        ACCEPT_INPUT = False

if __name__ == '__main__':
    start_time = time.time()
#   Parse args and read data
    opts = parse_args()
    cluster = Cluster([opts.cassandra_url])
    markov_session = cluster.connect(MARKOV_KEYSPACE)
    insert_sentence = markov_session.prepare("INSERT INTO markov.corpus (sentence_id, corpus_name, sentence_text) VALUES (?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    with open(opts.load_file, 'r') as f:
        corpus_text = f.read()

#   Split text into sentences
    corpus_lines = sent_tokenize(corpus_text)
    log(len(corpus_lines), 'Corpus Size')
    cnt = 0
    for row in corpus_lines:
#       remove newlines and extended whitespace from sentence
        tmp = ' '.join(row.split('\n'))
        if len(tmp.split()) <= 2:
            continue
        sentence = ' '.join(tmp.split())
        sentence_id = uuid.uuid4()

        if opts.mini_run and cnt > MINI_NUM_RECS:
            log(cnt, 'Record Count')
            log(time.time() - start_time, 'Execution Time')
            cluster.shutdown()
            sys.exit()

        if not opts.dry_run and ACCEPT_INPUT and len(sentence) > 2:
            log(sentence,  'Adding sentence to batch [' + str(sentence_id) +']')
            batch.add(insert_sentence, (sentence_id, opts.corpus, sentence))
        cnt = cnt + 1
        check_start_finish(sentence)

    if not opts.dry_run:
        markov_session.execute(batch) 
    log(cnt, 'Record Count')
    log(time.time() - start_time, 'Execution Time')
    cluster.shutdown()
