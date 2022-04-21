import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.util.Properties

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

  import scala.jdk.CollectionConverters._

  consumer.subscribe(Seq("output").asJava)

}