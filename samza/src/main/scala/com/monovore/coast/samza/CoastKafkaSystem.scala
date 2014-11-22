package com.monovore.coast.samza

import kafka.producer.{KeyedMessage, Producer => KafkaProducer}
import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system._
import org.apache.samza.system.kafka.KafkaSystemFactory
import org.apache.samza.util.{KafkaUtil, Logging}

import scala.collection.mutable.ArrayBuffer

object CoastKafkaSystem {

  type Bytes = Array[Byte]

  class Producer(producer: KafkaProducer[Bytes, Bytes], delayForStream: (String => Int)) extends SystemProducer with Logging {

    var buffers: Map[String, Ratchet] = Map.empty[String, Ratchet]

    var produceThread: Thread = null

    override def start(): Unit = {

      val runProducer: Runnable = new Runnable {

        override def run(): Unit = {

          while(true) {

            if (Thread.interrupted()) throw new InterruptedException()

            val nextBatch = buffers.valuesIterator.toList
              .flatMap { _.poll().getOrElse(Nil) }

            if (nextBatch.size > 0) {
              trace(s"Sending ${nextBatch.size} messages")
              producer.send(nextBatch: _*)
              trace(s"Messages sent.")
            } else {
              Thread.sleep(50)
            }
          }
        }
      }

      produceThread = new Thread(runProducer, "coast-kafka-produce-thread")

      produceThread.setDaemon(true)

      produceThread.start()
    }

    override def stop(): Unit = {

      if (produceThread != null) {
        produceThread.interrupt()
        produceThread.join(1000)
      }

      producer.close()
    }

    override def flush(source: String): Unit = {

      require(produceThread.getState != Thread.State.TERMINATED, "can't flush if producer's thread is not running")

      buffers(source).waitForFlush()
    }

    override def register(source: String): Unit = synchronized {
      buffers += (source -> new Ratchet)
    }

    override def send(source: String, envelope: OutgoingMessageEnvelope): Unit = {

      val stream = envelope.getSystemStream.getStream

      val message = new KeyedMessage(
        stream,
        envelope.getKey.asInstanceOf[Bytes],
        envelope.getPartitionKey,
        envelope.getMessage.asInstanceOf[Bytes]
      )

      buffers(source).add(delayForStream(stream), message)
    }
  }

  /**
   * The Ratchet guarantees:
   * - if add(n, a) happens before add(m, b), and n < m, then a will be written
   *   before b
   * - if add(n, a) happens before add(n, b), and a and b are in the same
   *   partition, then a will be written before b
   */
  class Ratchet {

    var buffers: Seq[ArrayBuffer[KeyedMessage[Bytes, Bytes]]] = Seq.empty

    def add(delay: Int, message: KeyedMessage[Bytes, Bytes]): Unit = synchronized {

      while (buffers.size <= delay) {
        buffers :+= ArrayBuffer()
      }

      buffers(delay) += message
    }

    def waitForFlush(): Unit = synchronized {

      while (buffers.nonEmpty) this.wait(10)
    }

    def poll(): Option[Seq[KeyedMessage[Bytes, Bytes]]] = synchronized {

      val head = buffers.headOption

      buffers = buffers.drop(1)

      this.notifyAll()

      head
    }
  }
}

class CoastKafkaSystemFactory extends SystemFactory with Logging {

  private[this] val kafkaFactory = new KafkaSystemFactory

  override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry): SystemConsumer =
    kafkaFactory.getConsumer(systemName, config, registry)

  override def getAdmin(systemName: String, config: Config): SystemAdmin =
    kafkaFactory.getAdmin(systemName, config)

  override def getProducer(systemName: String, config: Config, registry: MetricsRegistry): SystemProducer = {

    import org.apache.samza.config.KafkaConfig._

    val clientId = KafkaUtil.getClientId("samza-producer", config)

    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)

    require(producerConfig.producerType == "sync", "coast's kafka producer does its own message buffering")

    require(producerConfig.messageSendMaxRetries == 0, "messages can be duplicated or reordered with retries")

    require(producerConfig.requestRequiredAcks > 0, "not requiring acks makes failures invisible")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig)

    val delays =
      config.get(s"systems.$CoastSystem.delays").split(",")
        .map { _.split("/") }
        .map { case Array(name, delay) => name -> delay.toInt }
        .toMap
        .withDefaultValue(0)

    info(s"Configuring coast kafka system with delays: $delays")

    new CoastKafkaSystem.Producer(producer, delayForStream = delays)
  }
}

