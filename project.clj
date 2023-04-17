(defproject kafka-clojure-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [
                 [org.clojure/clojure "1.11.1"]
                 ;; Find online and add
                 [org.apache.kafka/kafka-clients "3.4.0"]
                 [org.slf4j/slf4j-api "1.7.32"]
                 [ch.qos.logback/logback-classic "1.2.6"]
                 ]
  ;:main ^:skip-aot kafka-clojure-example.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}}
  ;; Find online and add
  :aliases {"consumer" ["run" "-m" "io.confluent.examples.clients.clj.consumer"]
            "producer" ["run" "-m" "io.confluent.examples.clients.clj.producer"]}
  )
