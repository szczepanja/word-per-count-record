import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.time.Duration
import java.util
import java.util.{Locale, Properties}
import scala.io.StdIn.readLine

object WordCountPerLineApp extends App {

  val producerProps = new Properties()
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer")

  val producer = new KafkaProducer[String, String](producerProps)
  val consumer = new KafkaConsumer[String, String](consumerProps)

  while (true) {
    producer.send(new ProducerRecord[String, String]("word-counter", readLine("Enter string: ")))
  }

  import scala.jdk.CollectionConverters._

  consumer.subscribe(Seq("word-counter").asJava)

  def wordCounter(str: String) = {
    str.split("\\s+")
      .foldLeft(Map.empty[String, Int]) {
      (acc, word) => acc + (word -> (acc.getOrElse(word, 0) + 1))
    }
  }

}
