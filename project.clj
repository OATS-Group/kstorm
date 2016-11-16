(defproject kstorm "0.0.1-SNAPSHOT"
  :java-source-paths ["src/java"]
  :aot :all
  :resource-paths ["_resources"]
  :target-path "_build"
  :min-lein-version "2.0.0"
  :jvm-opts ["-client"]
  :dependencies  [[org.apache.storm/storm-core "1.0.2"]
                  [org.apache.storm/flux-core "1.0.2"]
                  [org.apache.storm/storm-kafka "1.0.2" :exclusions [org.slf4j/slf4j-api]]
                  [org.apache.kafka/kafka_2.11 "0.10.0.1" :exclusions [com.sun.jmx/jmxri com.sun.jdmk/jmxtools javax.jms/jms org.slf4j/slf4j-api]]
                  [org.apache.zookeeper/zookeeper "3.4.6" :exclusions [io.netty/netty org.slf4j/slf4j-api org.slf4j/slf4j-log4j12]]]
  :jar-exclusions     [#"log4j\.properties" #"org\.apache\.storm\.(?!flux)" #"trident" #"META-INF" #"meta-inf" #"\.yaml"]
  :uberjar-exclusions [#"log4j\.properties" #"org\.apache\.storm\.(?!flux)" #"trident" #"META-INF" #"meta-inf" #"\.yaml"]
  )
