
(ns kafka-clojure-example.draft-v1-kafka
  (:gen-class)
  (:import (java.time Duration)
           (java.util Arrays Properties)
           (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
           (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
           ))

;; create properties object for producer
(def producer-prop (Properties.))
(.setProperty producer-prop ProducerConfig/BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
(.setProperty producer-prop ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
(.setProperty producer-prop ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")


;; create properties object for consumer
(def consumer-prop (Properties.))
(.setProperty consumer-prop ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
(.setProperty consumer-prop ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer")
(.setProperty consumer-prop ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer")
(.setProperty consumer-prop ConsumerConfig/GROUP_ID_CONFIG "clojure_example_group")

;; how to create producer?
(def producer (KafkaProducer. producer-prop))
;; producer start running

(def topic "example-topic")
(def k "example-key")
(def v "example-value: hello world")

(def record (ProducerRecord. topic k v))

(.send producer record)

(.close producer)

;; how to create consumer
(def consumer (KafkaConsumer. consumer-prop))
(.subscribe consumer [topic])

(let [records (.poll consumer (Duration/ofMillis 1000))
      _       (def my-last-records records)]
  (println "key: " (.key records))
  )
(loop []
  (let [records (.poll consumer (Duration/ofMillis 1000))]
    (doseq [record records]
      (println "key:" (.key record) "value:" (.value record)))
    (recur)))

;; Rich Commenting Block
(comment
  producer-prop
  #_=> {"bootstrap.servers" "127.0.0.1:9092",
        "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer",
        "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer"}

  producer
  #_=> #object[org.apache.kafka.clients.producer.KafkaProducer
          0x644b78c4
          "org.apache.kafka.clients.producer.KafkaProducer@644b78c4"]

  consumer-prop
  #_=> {"key.deserializer"   "org.apache.kafka.common.serialization.StringDeserializer",
        "bootstrap.servers"  "127.0.0.1:9092",
        "group.id"           "clojure_example_group",
        "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}

  [topic k v]
  #_=> ["example-topic" "example-key" "example-value: hello world"]

  record
  #_=> #object[org.apache.kafka.clients.producer.ProducerRecord
               0x594f017a
               "ProducerRecord(topic=example-topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=example-key, value=example-value: hello world, timestamp=null)"]
  consumer
  #_=> #object[org.apache.kafka.clients.consumer.KafkaConsumer
               0x29e2e58
               "org.apache.kafka.clients.consumer.KafkaConsumer@29e2e58"]

  (type my-last-records)
  #_=> org.apache.kafka.clients.consumer.ConsumerRecords

  (iterator-seq my-last-records)
  )
