(ns kafka-clojure-example.draft-v2-kafka
  (:gen-class)
  (:import (java.time Duration)
           (org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)
           (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)))

(def kafka-client-config (atom {:producer {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG      "127.0.0.1:9092"
                                           ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringSerializer"
                                           ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"}
                                :consumer {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        "127.0.0.1:9092"
                                           ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringDeserializer"
                                           ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.k∏afka.common.serialization.StringDeserializer"
                                           ConsumerConfig/GROUP_ID_CONFIG                 "clojure_example_group"}}))

(def client (atom {:producer nil
                   :consumer nil}))


(def msg-count (atom 0))

(defn count-msg!
  []
  (swap! msg-count inc))



(defn build-producer
  []
  (let [producer-props {"bootstrap.servers" "127.0.0.1:9092"
                        "value.serializer"  StringSerializer
                        "key.serializer"    StringSerializer}]
    (KafkaProducer. producer-props)))

(defn build-consumer
  []
  (let [consumer-props {"bootstrap.servers",  "127.0.0.1:9092"
                        "group.id",           "clojure_example_group"
                        "key.deserializer",   StringDeserializer
                        "value.deserializer", StringDeserializer}]
    (KafkaConsumer. consumer-props)))



(defn client-producer!
  [client]
  (swap! client assoc :producer (build-producer)))

(defn client-consumer!
  [client]
  (swap! client assoc :consumer (build-consumer)))

(defn send-record
  [client]
  (let [producer        (:producer @client)
        producer-record (ProducerRecord. "example-topic" "example-key" "example-value: hello world!")]
    (.send producer producer-record)))


(defn consume-message
  [consumer]
  (let [records     (poll. consumer (Duration/ofMillis 1000))
        seq-records (iterator-seq (.iterator records))]
    (println "key: " map #(.key %) seq-records "\n value: " map #(.value %) seq-records)))


;; Rich Commenting Block
(comment

  ;(mount/stop)
  ;(mount/start)

  (type kafka-client-config)
  #_=> clojure.lang.Atom

  (:producer @kafka-client-config)
  #_=>
  {"bootstrap.servers" "127.0.0.1:9092",
   "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer",
   "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"}

  (type (:producer @kafka-client-config))
  #_=> clojure.lang.PersistentArrayMap

  (build-producer)
  ;; start running
  (build-consumer)
  #_=> #object[org.apache.kafka.clients.consumer.KafkaConsumer
               0x75161573
               "org.apache.kafka.clients.consumer.KafkaConsumer@75161573"]

  @client
  #_=> {:producer nil, :consumer nil}

  (client-consumer! client)
  #_=>
  {:producer nil,
   :consumer #object[kafka_clojure_example.draft_v2_kafka$build_consumer
                     0x1f20223b
                     "kafka_clojure_example.draft_v2_kafka$build_consumer@1f20223b"]}

  (client-producer! client)
  #_=>
  {:producer #object[kafka_clojure_example.draft_v2_kafka$build_producer
                     0x2283d59c
                     "kafka_clojure_example.draft_v2_kafka$build_producer@2283d59c"],
   :consumer #object[kafka_clojure_example.draft_v2_kafka$build_consumer
                     0x1f20223b
                     "kafka_clojure_example.draft_v2_kafka$build_consumer@1f20223b"]}

  ;(.send (:producer @client) (ProducerRecord. "example-topic" "example-key" "example-value: hello world!"))

  (count-msg!)


  (send-record client (str "Hello World" (count-msg!)))

  ;; producer functions that may not be useful:
  (defn close-producer
    [producer]
    (.close producer))

  (consume-message (:consumer @client))

  (let [consumer           (:consumer @client)
        consumer-subscribe (.subscribe consumer "example-topic")]
    (consume-message consumer))




  )

