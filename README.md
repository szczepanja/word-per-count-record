# word-per-count-record

## Exercise: [Word Count Per Record](https://jaceklaskowski.github.io/kafka-workshop/exercises/kafka-exercise-Word-Count-Per-Record.html)

Write a new Kafka application WordCountPerLineApp (using [Kafka Producer](https://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html)
and [Consumer APIs](https://kafka.apache.org/26/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html))
that does the following:

1. Consume records from a topic, e.g. `input` .
2. Count words (in the value of a record).
3. Produce records with the unique words and their occurrences _(counts)_.
   1. A record `key -> hello hello world` gives a record with the following value `hello -> 2, world -> 1` (and the same key as in the input record).

**(EXTRA)** 

4. Produces as many records as there are unique words in the input record with their occurrences (counts)
   1. A record `key -> hello hello world` gives two records in the output, i.e.
   `(hello, 2) and (world, 1 (as (key, value))`
