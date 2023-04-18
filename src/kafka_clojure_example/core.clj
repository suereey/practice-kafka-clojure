(ns kafka-clojure-example.core
  (:gen-class)
  (:import (java.time Duration)
           (java.util Arrays Properties)
           (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
           (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)))

;;forward declare functions that are used in atoms
;; (declear function-name)

(def kafka-client-config (atom {:producer {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG      "127.0.0.1:9092"
                                           ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringSerializer"
                                           ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"}
                                :consumer {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        "127.0.0.1:9092"
                                           ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringDeserializer"
                                           ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                                           ConsumerConfig/GROUP_ID_CONFIG                 "clojure_example_group"}}))

(defn producer-config
  [kafka-client-config]
  (let [producer-config (:producer @kafka-client-config)
        props           (Properties.)]
    (.setProperty props ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (producer-config "bootstrap.servers"))
    (.setProperty props ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (producer-config "key.serializer"))
    (.setProperty props ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (producer-config "value.serializer"))
    props))

(defn consumer-config
  [kafka-client-config]
  (let [consumer-config (:consumer @kafka-client-config)
        props           (Properties.)]
    (.setProperty props ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG (consumer-config "bootstrap.servers"))
    (.setProperty props ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (consumer-config "key.deserializer"))
    (.setProperty props ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (consumer-config "value.deserializer"))
    (.setProperty props ConsumerConfig/GROUP_ID_CONFIG (consumer-config "group.id"))
    props))

(defn consumer-config
  [kafka-client-config]
  (let [consumer-config (:consumer @kafka-client-config)
        props           (Properties.)]
    (.setProperty props ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG (consumer-config "bootstrap.servers"))
    (.setProperty props ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (consumer-config "key.deserializer"))
    (.setProperty props ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (consumer-config "value.deserializer"))
    (.setProperty props ConsumerConfig/GROUP_ID_CONFIG (consumer-config "group.id"))
    props))

(defn consumer-config
  [kafka-client-config]
  (let [props (doto (Properties.)
                (.setProperty ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092")
                (.setProperty ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
                (.setProperty ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"))]
    props))


(def client (atom {:producer nil
                   :consumer nil}))

(defn producer
  [props]
  (KafkaProducer. props))


(defn update-producer-client!
  [kafka-client-config client]
  (swap! client :producer (let [producer-props (producer-config kafka-client-config)
                                producer (producer producer-props)]
                            producer)))


(defn consumer
  [props]
  (KafkaConsumer. props))

(defn update-consumer-client!
  [kafka-client-config client]
  (swap! client :consumer (let [consumer-props (consumer-config kafka-client-config)
                                consumer (consumer consumer-props)]
                            consumer)))

(defn consume-message
  [consumer]
  (let [records     ( consumer (Duration/ofMillis 1000))
        seq-records (iterator-seq (.iterator records))]
    (println "key: " map #(.key %) seq-records "\n value: " map #(.value %) seq-records)))


;; Rich Commenting Block
(comment
  (type kafka-client-config)
  #_=> clojure.lang.Atom

  (:producer @kafka-client-config)
  #_=>
  {"bootstrap.servers" "127.0.0.1:9092",
   "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer",
   "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"}

  (type (:producer @kafka-client-config))
  #_=> clojure.lang.PersistentArrayMap

  ({"bootstrap.servers" "127.0.0.1:9092",
    "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"} "bootstrap.servers")
  #_=> "127.0.0.1:9092"

  ((:producer @kafka-client-config) "bootstrap.servers")
  #_=> "127.0.0.1:9092"

  ((:producer @kafka-client-config) "key.serializer")
  #_=> "org.apache.kafka.common.serialization.StringSerializer"

  ((:producer @kafka-client-config) "value.serializer")
  #_=> "org.apache.kafka.common.serialization.StringSerializer"

  ;; this one works
  (let [props (Properties.)]
    (.setProperty props ProducerConfig/BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    (.setProperty props ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
    (.setProperty props ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
    props)

  ;; this one also works, if i want to put things into the let binding
  (let [props (doto (Properties.)
                (.setProperty ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092")
                (.setProperty ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
                (.setProperty ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"))]
    props)

  ;; why this one is not working?
  (let [props (-> (Properties.)
                  (.setProperty ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092")
                  (.setProperty ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
                  (.setProperty ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"))]
    props)

  (producer-config kafka-client-config)
  #_=> {"bootstrap.servers" "127.0.0.1:9092",
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer",
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"}

  (:consumer @kafka-client-config)
  #_=>
  {"bootstrap.servers" "127.0.0.1:9092",
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer",
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer",
   "group.id" "clojure_example_group"}

  (consumer-config kafka-client-config)
  #_=>
  {"key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer",
   "bootstrap.servers" "127.0.0.1:9092",
   "group.id" "clojure_example_group",
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}

  (type (consumer-config kafka-client-config))
  #_=> java.util.Properties

  client
  #_=> #object[clojure.lang.Atom 0x2c8c63a3 {:status :ready, :val {:producer nil, :consumer nil}}]
  @client@client
  #_=> {:producer nil, :consumer nil}
  (:producer @client)
  #_=> nil
  (:consumer @client)
  #_=> nil

  (let [producer-props (producer-config kafka-client-config)
        producer (producer producer-props)]
    producer)
  ;; producer start running

  ;; test update client
  (assoc @client :producer 1)
  #_=> {:producer 1, :consumer nil}
  @client
  #_=> {:producer nil, :consumer nil}

  (swap! client assoc :producer 1)
  #_=> {:producer 1, :consumer nil}
  @client
  #_=> {:producer 1, :consumer nil}

  (swap! @client assoc :producer 2)
  ;; Execution error (ClassCastException)

  (swap! client :producer (let [producer-props (producer-config kafka-client-config)
                                   producer (producer producer-props)]
                               producer))
  ;; producer starts to run

  (update-producer-client! kafka-client-config client)
  ;; producer starts to run

  ;; question 2. why now client return 1?
  @client
  #_=> 1

  ;; run producer
  (update-producer-client! kafka-client-config client)

  ;; send record
  (let [producer (:producer @client)
        producer-record (ProducerRecord. "example-topic" "example-key" "example-value: hello world!")
        send-record (.send producer producer-record)]
    send-record)


  ;; producer functions that may not be useful:
  (defn close-producer
    [producer]
    (.close producer))

  (consumer-config kafka-client-config)
  #_=>
  {"key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer",
   "bootstrap.servers" "127.0.0.1:9092",
   "group.id" "clojure_example_group",
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}

  (consumer (consumer-config kafka-client-config))
  #_=> #object[org.apache.kafka.clients.consumer.KafkaConsumer
          0x6f2ef34b
          "org.apache.kafka.clients.consumer.KafkaConsumer@6f2ef34b"]

  @client
  #_=> {:producer nil, :consumer nil}

  (update-consumer-client! kafka-client-config client)

  ;; don't think this is right.
  @client
  #_=> nil

  (let [producer (:producer @client)
        producer-record (ProducerRecord. "example-topic" "example-key" "example-value: hello world!")
        send-record (.send producer producer-record)]
    send-record)

  (let [consumer (:consumer @client)
        consumer-subscribe (.subscribe consumer "example-topic")
        consume-message (consumer-message consumer)]
    consume-message)

  )

