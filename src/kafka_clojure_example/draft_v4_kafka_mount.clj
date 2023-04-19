(ns kafka-clojure-example.draft-v4-kafka-mount
  (:gen-class)
  (:import (java.time Duration)
           (org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)
           (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)))

(require '[mount.core :as mount])


;; apply mount to client/ producer & consumer /build

(def kafka-client-config (atom {:producer {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG      "127.0.0.1:9092"
                                           ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringSerializer"
                                           ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"}
                                :consumer {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        "127.0.0.1:9092"
                                           ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringDeserializer"
                                           ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                                           ConsumerConfig/GROUP_ID_CONFIG                 "clojure_example_group"}}))

(def client (atom {:producer nil
                   :consumer nil}))


(defn build-producer
  [kafka-client-config]
  (let [producer-config (:producer @kafka-client-config)
        props           {"bootstrap.servers" (get producer-config "bootstrap.servers")
                         "value.serializer"  (get producer-config "key.serializer")
                         "key.serializer"    (get producer-config "value.serializer")}]
    (KafkaProducer. props)))


(defn build-consumer
  [kafka-client-config]
  (let [consumer-config (:consumer @kafka-client-config)
        props           {"bootstrap.servers",  (get consumer-config "bootstrap.servers")
                         "group.id",           (get consumer-config "group.id")
                         "key.deserializer",   (get consumer-config "key.deserializer")
                         "value.deserializer", (get consumer-config "value.deserializer")}]
    (KafkaConsumer. props)))

(defn update-atom-map
  [k v]
  (fn [atom-map]
    (assoc atom-map k v)))

(defn update-producer-client!
  [client]
  (swap! client (update-atom-map :producer (build-producer kafka-client-config))))

(defn update-consumer-client!
  [client]
  (swap! client (update-atom-map :consumer (build-consumer kafka-client-config))))

(defn send-message
  [producer message]
  (let [producer-record (ProducerRecord. "example-topic" "example-key" message)]
    (.send producer producer-record)))

(defn consume-message [consumer]
  (.subscribe consumer ["example-topic"])
  (let [records     (.poll consumer (Duration/ofMillis 1000))
        seq-records (iterator-seq (.iterator records))]
    {"key: " (map #(.key %) seq-records) "value: " (map #(.value %) seq-records)}))


;; Rich Commenting Block
(comment

  (require '[mount.core :as mount])
  #_ => nil
  @client
  #_=> {:producer nil, :consumer nil}

  (:producer @kafka-client-config)
  #_=>
  {"bootstrap.servers" "127.0.0.1:9092",
   "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer",
   "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"}


  (build-producer kafka-client-config)
  #_=> #object[org.apache.kafka.clients.producer.KafkaProducer
               0x6db5e526
               "org.apache.kafka.clients.producer.KafkaProducer@6db5e526"]

  (build-consumer kafka-client-config)
  #_=> #object[org.apache.kafka.clients.consumer.KafkaConsumer
               0x496cb70
               "org.apache.kafka.clients.consumer.KafkaConsumer@496cb70"]

  @client
  #_=> {:producer nil, :consumer nil}

  (update-producer-client! client)
  #_=> {:producer #object[org.apache.kafka.clients.producer.KafkaProducer
                          0x60bb7f9
                          "org.apache.kafka.clients.producer.KafkaProducer@60bb7f9"],
        :consumer nil}
  (update-consumer-client! client)
  #_=> {:producer #object[org.apache.kafka.clients.producer.KafkaProducer
                          0x60bb7f9
                          "org.apache.kafka.clients.producer.KafkaProducer@60bb7f9"],
        :consumer #object[org.apache.kafka.clients.consumer.KafkaConsumer
                          0x6bd36a75
                          "org.apache.kafka.clients.consumer.KafkaConsumer@6bd36a75"]}

  @client
  #_=> {:producer #object[org.apache.kafka.clients.producer.KafkaProducer
                          0x60bb7f9
                          "org.apache.kafka.clients.producer.KafkaProducer@60bb7f9"],
        :consumer #object[org.apache.kafka.clients.consumer.KafkaConsumer
                          0x6bd36a75
                          "org.apache.kafka.clients.consumer.KafkaConsumer@6bd36a75"]}


  (send-message (:producer @client) "hello world")
  #_=> #object[org.apache.kafka.clients.producer.internals.FutureRecordMetadata
               0xd60be16
               "org.apache.kafka.clients.producer.internals.FutureRecordMetadata@d60be16"]

  (consume-message (:consumer @client))
  #_=>
  {"key: "   ("example-key" "example-key" "example-key" "example-key"),
   "value: " ("hello world" "hello world" "hello world" "hello world")}


  ;; producer functions that may not be useful:
  (defn close-producer
    [producer]
    (.close producer))

  ;; figure out how to use the following functions
  (def msg-count (atom 0))

  (defn count-msg!
    []
    (swap! msg-count inc))

  ;(mount/stop)
  ;(mount/start)
  )

