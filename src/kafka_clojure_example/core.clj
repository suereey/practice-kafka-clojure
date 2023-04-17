(ns kafka-clojure-example.core
  (:gen-class)
  (:import (java.time Duration)
           (java.util Arrays Properties)
           (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
           (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
           ))

(def bootstrap-server (atom "127.0.0.1:9092"))
(def key-serializer (atom "org.apache.kafka.common.serialization.StringSerializer"))
(def value-serializer (atom "org.apache.kafka.common.serialization.StringSerializer"))
(def key-deserializer (atom "org.apache.kafka.common.serialization.StringDeserializer"))
(def value-deserializer (atom "org.apache.kafka.common.serialization.StringDeserializer"))
(def group-id (atom "clojure_example_group"))

(def topic (atom "example-topic"))
(def k (atom "example-key"))
(def v (atom "example-value: hello world!"))

(def duration (atom (Duration/ofMillis 1000)))

(defn producer-prop
  [bootstrap-server key-serializer value-serializer]
  (let [props (Properties.)]
    (.setProperty props ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (deref bootstrap-server))
    (.setProperty props ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (deref key-serializer))
    (.setProperty props ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (deref value-serializer))
    props))

(defn consumer-prop
  [bootstrap-server key-deserializer value-deserializer group_id]
  (let [props (Properties.)]
    (.setProperty props ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG (deref bootstrap-server))
    (.setProperty props ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (deref key-deserializer))
    (.setProperty props ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (deref value-deserializer))
    (.setProperty props ConsumerConfig/GROUP_ID_CONFIG (deref group_id))
    props))

(defn producer
  [props]
  (KafkaProducer. props))

(defn consumer
  [props]
  (KafkaConsumer. props))

;; record
(defn record
  [topic k v]
  (ProducerRecord. (deref topic) (deref k) (deref v)))

(defn producer-send-record
  [producer record]
  (.send producer record))

(defn close-producer
  [producer]
  (.close producer))

(defn consumer-subscribe
  [topic consumer]
  (.subscribe consumer [(deref topic)]))

(defn consumer-message
  [consumer duration]
  (let [records (.poll consumer (deref duration))]
    (doseq [record records]
      (println "message is: " (.value record)))))


(defn consumer-message
  [consumer duration]
  (let [records (.poll consumer (deref duration))]))



(def clients nil)

;; how to create consumer

(.subscribe consumer [topic])

(let [records (.poll consumer (Duration/ofMillis 1000))
      _       (def my-last-records records)]
  (println "key: " (.key records))

  )


(type my-last-records)

;; Rich Commenting Block
(comment

  [bootstrap-server key-serializer value-serializer]
  #_=>
  [#object[clojure.lang.Atom 0x7a5ca2ff {:status :ready, :val "127.0.0.1:9092"}]
   #object[clojure.lang.Atom 0x149e60f9 {:status :ready, :val "org.apache.kafka.common.serialization.StringSerializer"}]
   #object[clojure.lang.Atom 0x4fdd87d6 {:status :ready, :val "org.apache.kafka.common.serialization.StringSerializer"}]]

  (producer-prop bootstrap-server key-serializer value-serializer)
  #_=>
  {"bootstrap.servers" "127.0.0.1:9092",
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer",
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"}

  [bootstrap-server key-deserializer value-deserializer group-id]
  #_=>
  [#object[clojure.lang.Atom 0x7a5ca2ff {:status :ready, :val "127.0.0.1:9092"}]
   #object[clojure.lang.Atom 0x5bd51056 {:status :ready, :val "org.apache.kafka.common.serialization.StringDeserializer"}]
   #object[clojure.lang.Atom 0x346de55f {:status :ready, :val "org.apache.kafka.common.serialization.StringDeserializer"}]
   #object[clojure.lang.Atom 0x7bac7ec2 {:status :ready, :val "clojure_example_group"}]]

  (consumer-prop bootstrap-server key-deserializer value-deserializer group-id)
  #_=>
  {"key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer",
     "bootstrap.servers" "127.0.0.1:9092",
     "group.id" "clojure_example_group",
     "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}

  (producer {"bootstrap.servers" "127.0.0.1:9092",
             "value.serializer" "org.apache.kafka.common.serialization.StringSerializer",
             "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"})
  (consumer {"key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer",
             "bootstrap.servers" "127.0.0.1:9092",
             "group.id" "clojure_example_group",
             "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

  [topic k v]
  #_=>
  [#object[clojure.lang.Atom 0x99d37cc {:status :ready, :val "example-topic"}]
   #object[clojure.lang.Atom 0x5258de05 {:status :ready, :val "example-key"}]
   #object[clojure.lang.Atom 0x4de42d29 {:status :ready, :val "example-value: hello world!"}]]

  (def record (ProducerRecord. (deref topic) (deref k) (deref v)))
  #_=> #'kafka-clojure-example.core/record

  producer
  #_=> #object[kafka_clojure_example.core$producer 0x2e6a7a37 "kafka_clojure_example.core$producer@2e6a7a37"]

  record
  #_=> #object[org.apache.kafka.clients.producer.ProducerRecord
          0x55ebe6ff
          "ProducerRecord(topic=example-topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=example-key, value=example-value: hello world!, timestamp=null)"]

  ;; run producer
  (let [props (producer-prop bootstrap-server key-serializer value-serializer)
        producer (producer props)
        record (record topic k v)
        run-producer (.send producer record)]
    run-producer)

  (def consumer (consumer (consumer-prop bootstrap-server key-deserializer value-deserializer group-id)))

  (consumer-subscribe topic consumer)

  duration
  #_=> #object[clojure.lang.Atom 0x36cc27ea {:status :ready, :val #object[java.time.Duration 0x35aa72a1 "PT1S"]}]

  (consumer-message consumer duration)

  #_org.apache.kafka.clients.consumer.ConsumerRecords
  (let [records (.poll consumer (deref duration))]
   (println records))
  #_#object[org.apache.kafka.clients.consumer.ConsumerRecords 0xd8d5fbb org.apache.kafka.clients.consumer.ConsumerRecords@d8d5fbb]

  (let [records (.poll consumer (deref duration))]
    (println (type (iterator-seq (.iterator records)))))


  (let [records (.poll consumer (deref duration))
        seq-records (iterator-seq (.iterator records))]
    (println "key: " map #(.key %) seq-records)
    (println "value: " map #(.value %) seq-records))

  )
