import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringDeserializer, StringSerializer}

import java.time.Duration
import java.util.Properties

object WordCountPerLineApp extends App {

  val props = new Properties()
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer")
  val consumer = new KafkaConsumer[String, String](props)


  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer])
  val producer = new KafkaProducer[String, Int](props)

  import scala.jdk.CollectionConverters._

  consumer.subscribe(Seq("word-counter").asJava)

  while (true) {
    val records = consumer.poll(Duration.ofMillis(100)).asScala
    records.foreach { record =>
      record.value()
        .split("\\W+")
        .groupBy(identity)
        .view
        .mapValues(_.length)
        .foreach { case r@(word, occ) =>
          println(r)
          producer.send(new ProducerRecord[String, Int]("word-counter", word, occ))
        }
    }
  }
}