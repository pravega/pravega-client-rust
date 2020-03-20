
running 1 test
Listening for transport dt_socket at address: 8000
16:38:08,650 |-INFO in ch.qos.logback.classic.LoggerContext[default] - Found resource [/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/conf/logback.xml] at [file:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/conf/logback.xml]
16:38:08,917 |-INFO in ch.qos.logback.classic.joran.action.ConfigurationAction - debug attribute not set
16:38:08,930 |-INFO in ch.qos.logback.core.joran.action.AppenderAction - About to instantiate appender of type [ch.qos.logback.core.ConsoleAppender]
16:38:08,935 |-INFO in ch.qos.logback.core.joran.action.AppenderAction - Naming appender as [consoleAppender]
16:38:08,946 |-INFO in ch.qos.logback.core.joran.action.NestedComplexPropertyIA - Assuming default type [ch.qos.logback.classic.encoder.PatternLayoutEncoder] for [encoder] property
16:38:09,027 |-INFO in ch.qos.logback.core.joran.action.AppenderAction - About to instantiate appender of type [ch.qos.logback.core.rolling.RollingFileAppender]
16:38:09,032 |-INFO in ch.qos.logback.core.joran.action.AppenderAction - Naming appender as [FILE]
16:38:09,065 |-INFO in c.q.l.core.rolling.TimeBasedRollingPolicy@1636506029 - Will use gz compression
16:38:09,067 |-INFO in c.q.l.core.rolling.TimeBasedRollingPolicy@1636506029 - Will use the pattern /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/logs/pravega_%d{yyyy-MM-dd}.%i.log for the active file
16:38:09,071 |-INFO in ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP@2d3379b4 - The date pattern is 'yyyy-MM-dd' from file name pattern '/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/logs/pravega_%d{yyyy-MM-dd}.%i.log.gz'.
16:38:09,071 |-INFO in ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP@2d3379b4 - Roll-over at midnight.
16:38:09,076 |-INFO in ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP@2d3379b4 - Setting initial period to Thu Mar 19 16:37:22 PDT 2020
16:38:09,077 |-WARN in ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP@2d3379b4 - SizeAndTimeBasedFNATP is deprecated. Use SizeAndTimeBasedRollingPolicy instead
16:38:09,077 |-WARN in ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP@2d3379b4 - For more information see http://logback.qos.ch/manual/appenders.html#SizeAndTimeBasedRollingPolicy
16:38:09,082 |-INFO in ch.qos.logback.core.joran.action.NestedComplexPropertyIA - Assuming default type [ch.qos.logback.classic.encoder.PatternLayoutEncoder] for [encoder] property
16:38:09,086 |-INFO in ch.qos.logback.core.rolling.RollingFileAppender[FILE] - Active log file name: /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/logs/pravega.log
16:38:09,086 |-INFO in ch.qos.logback.core.rolling.RollingFileAppender[FILE] - File property is set to [/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/logs/pravega.log]
16:38:09,089 |-INFO in ch.qos.logback.classic.joran.action.RootLoggerAction - Setting level of ROOT logger to INFO
16:38:09,089 |-INFO in ch.qos.logback.core.joran.action.AppenderRefAction - Attaching appender named [consoleAppender] to Logger[ROOT]
16:38:09,090 |-INFO in ch.qos.logback.core.joran.action.AppenderRefAction - Attaching appender named [FILE] to Logger[ROOT]
16:38:09,090 |-INFO in ch.qos.logback.classic.joran.action.ConfigurationAction - End of configuration.
16:38:09,091 |-INFO in ch.qos.logback.classic.joran.JoranConfigurator@30c15d8b - Registering current configuration as safe fallback point

2020-03-19 16:38:09,221 604  [main] INFO  i.p.local.LocalPravegaEmulator - Starting Pravega Emulator with ports: ZK port 4000, controllerPort 9090, SegmentStorePort 6000
2020-03-19 16:38:09,244 627  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:zookeeper.version=3.5.5-390fe37ea45dee01bf87dc1c042b5e3dcce88653, built on 05/03/2019 12:07 GMT
2020-03-19 16:38:09,244 627  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:host.name=wenqi-VirtualBox
2020-03-19 16:38:09,244 627  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:java.version=1.8.0_181
2020-03-19 16:38:09,244 627  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:java.vendor=Oracle Corporation
2020-03-19 16:38:09,245 628  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:java.home=/usr/java/jdk1.8.0_181/jre
2020-03-19 16:38:09,245 628  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:java.class.path=/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-standalone-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-server-host-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-controller-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-client-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-server-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-storage-impl-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-bindings-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-storage-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-contracts-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-protocol-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-controller-api-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-metrics-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-security-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-cluster-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-common-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-authplugin-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-minicluster-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-common-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/logback-classic-1.2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-recipes-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-auth-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-framework-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-tests-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-resourcemanager-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-client-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-server-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/object-client-3.0.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-test-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-jobclient-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-jobclient-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-hs-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-app-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-shuffle-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-nodemanager-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-web-proxy-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-applicationhistoryservice-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-3.5.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-3.5.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-core-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/avro-1.7.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-registry-influx-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-common-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-proto-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/http-server-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/circe-checksum-4.7.3.nar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/smart-client-2.1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/object-transform-1.1.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-client-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-jersey2-jaxrs-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-jaxrs-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-core-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-stats-api-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/apacheds-i18n-2.0.0-M15.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/api-asn1-api-1.0.0-M20.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/api-util-1.0.0-M20.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-models-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/slf4j-api-1.7.25.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-container-grizzly2-http-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-container-servlet-core-2.25.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.ws.rs-api-2.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-tcnative-boringssl-static-2.0.17.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-io-2.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-api-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-protobuf-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-netty-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-stub-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-auth-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-protobuf-lite-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-core-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guava-28.1-jre.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-native-epoll-4.1.30.Final-linux-x86_64.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.servlet-api-4.0.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-lang3-3.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-hk2-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-json-jackson-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-runtime-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-core-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-json-1.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-impl-2.2.3-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-api-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/rocksdbjni-5.13.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-server-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-multipart-2.25.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-client-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-jaxb-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-common-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-locator-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-api-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-utils-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.inject-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-http-server-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-entity-filtering-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.ws.rs-api-2.1.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-annotations-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-cli-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-math3-3.1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/xmlenc-0.52.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jets3t-0.9.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-apache-client4-1.19.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/httpclient-4.5.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-codec-1.10.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-net-3.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-collections-3.2.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/servlet-api-2.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-sslengine-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-util-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsp-api-2.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-configuration-1.10.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-logging-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/log4j-1.2.17.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-lang-2.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-jaxrs-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-xc-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-mapper-asl-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-core-asl-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/protobuf-java-3.5.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/gson-2.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsch-0.1.54.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsr305-3.0.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/htrace-core4-4.0.1-incubating.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-compress-1.18.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-client-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-daemon-1.0.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-3.10.5.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-all-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/xercesImpl-2.9.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/leveldbjni-all-1.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/logback-core-1.2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/failureaccess-1.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/checker-qual-2.8.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/error_prone_annotations-2.3.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/j2objc-annotations-1.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/animal-sniffer-annotations-1.18.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-http2-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-handler-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-native-unix-common-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-handler-proxy-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-http-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-socks-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-buffer-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-resolver-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-common-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-registry-statsd-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-core-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jjwt-0.9.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-module-jaxb-annotations-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-databind-2.9.9.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-annotations-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/stax-ex-1.7.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/FastInfoset-1.2.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-collections4-4.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jcommander-1.48.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jna-3.2.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jdom2-2.0.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-http-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.annotation-api-1.3.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/osgi-resource-locator-1.0.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/validation-api-2.0.1.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-jute-3.5.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/audience-annotations-0.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/httpcore-4.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jettison-1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/java-xmlbuilder-0.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/paranamer-2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/snappy-java-1.0.4.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/nimbus-jose-jwt-4.41.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/okhttp-2.4.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-guice-1.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guice-servlet-3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/proto-google-common-protos-1.0.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/google-auth-library-credentials-0.9.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/HdrHistogram-2.1.11.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/LatencyUtils-2.0.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/reflections-0.9.11.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.inject-2.5.0-b32.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/mimepull-1.9.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/aopalliance-repackaged-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javassist-3.22.0-CR2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-dataformat-yaml-2.9.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-core-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/txw2-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/istack-commons-runtime-3.0.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-client-1.19.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/lzma-sdk-4j-9.22.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-framework-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/servlet-api-2.5-20081211.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jcip-annotations-1.0-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/json-smart-2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/okio-1.4.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guice-3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-context-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/opencensus-contrib-grpc-metrics-0.17.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/opencensus-api-0.17.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/accessors-smart-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.inject-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/aopalliance-1.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/cglib-2.2.1-v20090111.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/snakeyaml-1.23.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-annotations-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/asm-5.0.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/asm-3.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/pluginlib/*
2020-03-19 16:38:09,246 629  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:java.library.path=/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/build/backtrace-sys-b28453e14347046d/out:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/deps:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
2020-03-19 16:38:09,246 629  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:java.io.tmpdir=/tmp
2020-03-19 16:38:09,249 632  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:java.compiler=<NA>
2020-03-19 16:38:09,249 632  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:os.name=Linux
2020-03-19 16:38:09,249 632  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:os.arch=amd64
2020-03-19 16:38:09,250 633  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:os.version=4.15.0-91-generic
2020-03-19 16:38:09,250 633  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:user.name=wenqi
2020-03-19 16:38:09,250 633  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:user.home=/home/wenqi
2020-03-19 16:38:09,250 633  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:user.dir=/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test
2020-03-19 16:38:09,250 633  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:os.memory.free=126MB
2020-03-19 16:38:09,251 634  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:os.memory.max=3641MB
2020-03-19 16:38:09,251 634  [main] INFO  o.a.z.server.ZooKeeperServer - Server environment:os.memory.total=165MB
2020-03-19 16:38:09,278 661  [main] INFO  o.a.zookeeper.server.ZKDatabase - zookeeper.snapshotSizeFactor = 0.33
2020-03-19 16:38:09,281 664  [main] INFO  o.a.z.server.ZooKeeperServer - minSessionTimeout set to 6000
2020-03-19 16:38:09,281 664  [main] INFO  o.a.z.server.ZooKeeperServer - maxSessionTimeout set to 60000
2020-03-19 16:38:09,282 665  [main] INFO  o.a.z.server.ZooKeeperServer - Created server with tickTime 3000 minSessionTimeout 6000 maxSessionTimeout 60000 datadir /tmp/zookeeper2094388859860110127inproc/version-2 snapdir /tmp/zookeeper2094388859860110127inproc/version-2
2020-03-19 16:38:09,292 675  [main] INFO  o.a.z.server.ServerCnxnFactory - Using org.apache.zookeeper.server.NIOServerCnxnFactory as server connection factory
2020-03-19 16:38:09,292 675  [main] INFO  i.p.s.s.i.b.ZooKeeperServiceRunner - Starting Zookeeper server at localhost:4000 ...
2020-03-19 16:38:09,296 679  [main] INFO  o.a.z.server.NIOServerCnxnFactory - Configuring NIO connection handler with 10s sessionless connection timeout, 1 selector thread(s), 8 worker threads, and 64 kB direct buffers.
2020-03-19 16:38:09,305 688  [main] INFO  o.a.z.server.NIOServerCnxnFactory - binding to port localhost/127.0.0.1:4000
2020-03-19 16:38:09,325 708  [main] INFO  o.a.z.s.persistence.FileTxnSnapLog - Snapshotting: 0x0 to /tmp/zookeeper2094388859860110127inproc/version-2/snapshot.0
2020-03-19 16:38:09,330 713  [main] INFO  o.a.z.s.persistence.FileTxnSnapLog - Snapshotting: 0x0 to /tmp/zookeeper2094388859860110127inproc/version-2/snapshot.0
2020-03-19 16:38:09,474 857  [NIOWorkerThread-1] INFO  o.a.z.s.command.FourLetterCommands - The list of known four letter word commands is : [{1936881266=srvr, 1937006964=stat, 2003003491=wchc, 1685417328=dump, 1668445044=crst, 1936880500=srst, 1701738089=envi, 1668247142=conf, -720899=telnet close, 2003003507=wchs, 2003003504=wchp, 1684632179=dirs, 1668247155=cons, 1835955314=mntr, 1769173615=isro, 1920298859=ruok, 1735683435=gtmk, 1937010027=stmk}]
2020-03-19 16:38:09,475 858  [NIOWorkerThread-1] INFO  o.a.z.s.command.FourLetterCommands - The list of enabled four letter word commands is : [[wchs, stat, wchp, dirs, stmk, conf, ruok, mntr, srvr, wchc, envi, srst, isro, dump, gtmk, telnet close, crst, cons]]
2020-03-19 16:38:09,475 858  [NIOWorkerThread-1] INFO  o.a.zookeeper.server.NIOServerCnxn - Processing stat command from /127.0.0.1:51240
2020-03-19 16:38:09,480 863  [NIOWorkerThread-1] INFO  o.a.z.s.c.AbstractFourLetterCommand - Stat command output
2020-03-19 16:38:09,482 865  [main] INFO  o.a.bookkeeper.util.LocalBookKeeper - Server UP
2020-03-19 16:38:09,547 930  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:zookeeper.version=3.5.5-390fe37ea45dee01bf87dc1c042b5e3dcce88653, built on 05/03/2019 12:07 GMT
2020-03-19 16:38:09,547 930  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:host.name=wenqi-VirtualBox
2020-03-19 16:38:09,547 930  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:java.version=1.8.0_181
2020-03-19 16:38:09,547 930  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:java.vendor=Oracle Corporation
2020-03-19 16:38:09,547 930  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:java.home=/usr/java/jdk1.8.0_181/jre
2020-03-19 16:38:09,547 930  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:java.class.path=/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-standalone-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-server-host-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-controller-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-client-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-server-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-storage-impl-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-bindings-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-storage-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-contracts-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-protocol-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-controller-api-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-metrics-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-security-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-cluster-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-common-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-authplugin-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-minicluster-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-common-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/logback-classic-1.2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-recipes-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-auth-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-framework-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-tests-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-resourcemanager-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-client-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-server-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/object-client-3.0.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-test-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-jobclient-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-jobclient-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-hs-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-app-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-shuffle-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-nodemanager-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-web-proxy-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-applicationhistoryservice-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-3.5.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-3.5.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-core-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/avro-1.7.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-registry-influx-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-common-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-proto-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/http-server-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/circe-checksum-4.7.3.nar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/smart-client-2.1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/object-transform-1.1.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-client-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-jersey2-jaxrs-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-jaxrs-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-core-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-stats-api-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/apacheds-i18n-2.0.0-M15.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/api-asn1-api-1.0.0-M20.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/api-util-1.0.0-M20.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-models-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/slf4j-api-1.7.25.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-container-grizzly2-http-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-container-servlet-core-2.25.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.ws.rs-api-2.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-tcnative-boringssl-static-2.0.17.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-io-2.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-api-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-protobuf-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-netty-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-stub-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-auth-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-protobuf-lite-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-core-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guava-28.1-jre.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-native-epoll-4.1.30.Final-linux-x86_64.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.servlet-api-4.0.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-lang3-3.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-hk2-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-json-jackson-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-runtime-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-core-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-json-1.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-impl-2.2.3-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-api-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/rocksdbjni-5.13.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-server-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-multipart-2.25.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-client-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-jaxb-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-common-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-locator-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-api-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-utils-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.inject-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-http-server-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-entity-filtering-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.ws.rs-api-2.1.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-annotations-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-cli-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-math3-3.1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/xmlenc-0.52.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jets3t-0.9.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-apache-client4-1.19.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/httpclient-4.5.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-codec-1.10.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-net-3.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-collections-3.2.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/servlet-api-2.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-sslengine-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-util-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsp-api-2.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-configuration-1.10.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-logging-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/log4j-1.2.17.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-lang-2.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-jaxrs-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-xc-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-mapper-asl-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-core-asl-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/protobuf-java-3.5.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/gson-2.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsch-0.1.54.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsr305-3.0.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/htrace-core4-4.0.1-incubating.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-compress-1.18.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-client-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-daemon-1.0.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-3.10.5.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-all-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/xercesImpl-2.9.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/leveldbjni-all-1.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/logback-core-1.2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/failureaccess-1.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/checker-qual-2.8.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/error_prone_annotations-2.3.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/j2objc-annotations-1.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/animal-sniffer-annotations-1.18.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-http2-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-handler-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-native-unix-common-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-handler-proxy-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-http-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-socks-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-buffer-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-resolver-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-common-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-registry-statsd-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-core-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jjwt-0.9.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-module-jaxb-annotations-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-databind-2.9.9.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-annotations-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/stax-ex-1.7.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/FastInfoset-1.2.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-collections4-4.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jcommander-1.48.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jna-3.2.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jdom2-2.0.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-http-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.annotation-api-1.3.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/osgi-resource-locator-1.0.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/validation-api-2.0.1.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-jute-3.5.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/audience-annotations-0.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/httpcore-4.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jettison-1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/java-xmlbuilder-0.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/paranamer-2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/snappy-java-1.0.4.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/nimbus-jose-jwt-4.41.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/okhttp-2.4.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-guice-1.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guice-servlet-3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/proto-google-common-protos-1.0.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/google-auth-library-credentials-0.9.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/HdrHistogram-2.1.11.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/LatencyUtils-2.0.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/reflections-0.9.11.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.inject-2.5.0-b32.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/mimepull-1.9.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/aopalliance-repackaged-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javassist-3.22.0-CR2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-dataformat-yaml-2.9.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-core-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/txw2-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/istack-commons-runtime-3.0.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-client-1.19.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/lzma-sdk-4j-9.22.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-framework-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/servlet-api-2.5-20081211.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jcip-annotations-1.0-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/json-smart-2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/okio-1.4.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guice-3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-context-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/opencensus-contrib-grpc-metrics-0.17.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/opencensus-api-0.17.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/accessors-smart-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.inject-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/aopalliance-1.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/cglib-2.2.1-v20090111.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/snakeyaml-1.23.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-annotations-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/asm-5.0.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/asm-3.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/pluginlib/*
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:java.library.path=/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/build/backtrace-sys-b28453e14347046d/out:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/deps:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:java.io.tmpdir=/tmp
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:java.compiler=<NA>
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:os.name=Linux
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:os.arch=amd64
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:os.version=4.15.0-91-generic
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:user.name=wenqi
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:user.home=/home/wenqi
2020-03-19 16:38:09,548 931  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:user.dir=/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test
2020-03-19 16:38:09,549 932  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:os.memory.free=147MB
2020-03-19 16:38:09,549 932  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:os.memory.max=3641MB
2020-03-19 16:38:09,549 932  [main] INFO  org.apache.zookeeper.ZooKeeper - Client environment:os.memory.total=165MB
2020-03-19 16:38:09,583 966  [main] INFO  o.a.c.f.imps.CuratorFrameworkImpl - Starting
2020-03-19 16:38:09,592 975  [main] INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=localhost:4000 sessionTimeout=5000 watcher=org.apache.curator.ConnectionState@46dffdc3
2020-03-19 16:38:09,599 982  [main] INFO  o.apache.zookeeper.common.X509Util - Setting -D jdk.tls.rejectClientInitiatedRenegotiation=true to disable client-initiated TLS renegotiation
2020-03-19 16:38:09,604 987  [main] INFO  o.apache.zookeeper.ClientCnxnSocket - jute.maxbuffer value is 4194304 Bytes
2020-03-19 16:38:09,614 997  [main] INFO  org.apache.zookeeper.ClientCnxn - zookeeper.request.timeout value is 0. feature enabled=
2020-03-19 16:38:09,618 1001 [main-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server localhost/127.0.0.1:4000. Will not attempt to authenticate using SASL (unknown error)
2020-03-19 16:38:09,619 1002 [main-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Socket connection established, initiating session, client: /127.0.0.1:51242, server: localhost/127.0.0.1:4000
2020-03-19 16:38:09,626 1009 [main] INFO  o.a.c.f.imps.CuratorFrameworkImpl - Default schema
2020-03-19 16:38:09,627 1010 [SyncThread:0] INFO  o.a.z.server.persistence.FileTxnLog - Creating new log file: log.1
2020-03-19 16:38:09,650 1033 [main-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Session establishment complete on server localhost/127.0.0.1:4000, sessionid = 0x10006911fc30000, negotiated timeout = 6000
2020-03-19 16:38:09,656 1039 [main-EventThread] INFO  o.a.c.f.s.ConnectionStateManager - State change: CONNECTED
2020-03-19 16:38:09,669 1052 [main] WARN  i.p.local.InProcPravegaCluster - Not able to delete path /pravega . Exception KeeperErrorCode = NoNode for /pravega
2020-03-19 16:38:09,671 1054 [main-EventThread] INFO  o.a.c.f.imps.EnsembleTracker - New config event received: {}
2020-03-19 16:38:09,672 1055 [main-EventThread] INFO  o.a.c.f.imps.EnsembleTracker - New config event received: {}
2020-03-19 16:38:09,677 1060 [main] WARN  i.p.local.InProcPravegaCluster - Not able to delete path /hostIndex . Exception KeeperErrorCode = NoNode for /hostIndex
2020-03-19 16:38:09,680 1063 [main] WARN  i.p.local.InProcPravegaCluster - Not able to delete path /store . Exception KeeperErrorCode = NoNode for /store
2020-03-19 16:38:09,683 1066 [main] WARN  i.p.local.InProcPravegaCluster - Not able to delete path /taskIndex . Exception KeeperErrorCode = NoNode for /taskIndex
2020-03-19 16:38:09,684 1067 [Curator-Framework-0] INFO  o.a.c.f.imps.CuratorFrameworkImpl - backgroundOperationsLoop exiting
2020-03-19 16:38:09,801 1184 [main-EventThread] INFO  org.apache.zookeeper.ClientCnxn - EventThread shut down for session: 0x10006911fc30000
2020-03-19 16:38:09,801 1184 [main] INFO  org.apache.zookeeper.ZooKeeper - Session: 0x10006911fc30000 closed
2020-03-19 16:38:09,826 1209 [main] INFO  io.pravega.controller.util.Config - Controller configuration:
2020-03-19 16:38:09,828 1211 [main] INFO  io.pravega.controller.util.Config - sun.arch.data.model = 64
2020-03-19 16:38:09,828 1211 [main] INFO  io.pravega.controller.util.Config - java.specification.version = 1.8
2020-03-19 16:38:09,828 1211 [main] INFO  io.pravega.controller.util.Config - LS_COLORS = rs=0:di=01;34:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:mi=00:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arc=01;31:*.arj=01;31:*.taz=01;31:*.lha=01;31:*.lz4=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.tzo=01;31:*.t7z=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.dz=01;31:*.gz=01;31:*.lrz=01;31:*.lz=01;31:*.lzo=01;31:*.xz=01;31:*.zst=01;31:*.tzst=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.alz=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.cab=01;31:*.wim=01;31:*.swm=01;31:*.dwm=01;31:*.esd=01;31:*.jpg=01;35:*.jpeg=01;35:*.mjpg=01;35:*.mjpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=00;36:*.au=00;36:*.flac=00;36:*.m4a=00;36:*.mid=00;36:*.midi=00;36:*.mka=00;36:*.mp3=00;36:*.mpc=00;36:*.ogg=00;36:*.ra=00;36:*.wav=00;36:*.oga=00;36:*.opus=00;36:*.spx=00;36:*.xspf=00;36:
2020-03-19 16:38:09,828 1211 [main] INFO  io.pravega.controller.util.Config - SSH_AUTH_SOCK = /run/user/1000/keyring/ssh
2020-03-19 16:38:09,828 1211 [main] INFO  io.pravega.controller.util.Config - java.vendor = Oracle Corporation
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - HOME = /home/wenqi
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - GNOME_TERMINAL_SCREEN = /org/gnome/Terminal/screen/f2f2e7ad_2a10_4a79_8157_be5ff63fcc87
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - XMODIFIERS = @im=ibus
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - log.name = pravega
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - RUSTUP_HOME = /home/wenqi/.rustup
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - user.country = US
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - CLASS_PATH = .:/usr/java/jdk1.8.0_181/lib/dt.jar:/usr/java/jdk1.8.0_181/lib/tools.jar:/usr/java/jdk1.8.0_181/jre/lib
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - LD_LIBRARY_PATH = /home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/build/backtrace-sys-b28453e14347046d/out:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/deps:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - QT4_IM_MODULE = xim
2020-03-19 16:38:09,829 1212 [main] INFO  io.pravega.controller.util.Config - XDG_SESSION_ID = 1
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - sun.boot.class.path = /usr/java/jdk1.8.0_181/jre/lib/resources.jar:/usr/java/jdk1.8.0_181/jre/lib/rt.jar:/usr/java/jdk1.8.0_181/jre/lib/sunrsasign.jar:/usr/java/jdk1.8.0_181/jre/lib/jsse.jar:/usr/java/jdk1.8.0_181/jre/lib/jce.jar:/usr/java/jdk1.8.0_181/jre/lib/charsets.jar:/usr/java/jdk1.8.0_181/jre/lib/jfr.jar:/usr/java/jdk1.8.0_181/jre/classes
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - java.runtime.version = 1.8.0_181-b13
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - file.separator = /
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - LESSOPEN = | /usr/bin/lesspipe %s
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - java.vm.specification.vendor = Oracle Corporation
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - LANG = en_US.UTF-8
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - sun.java.launcher = SUN_STANDARD
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - WORKSPACE = /home/wenqi/Desktop/go/src/github.com/pravega/pravega-operator
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - SDKMAN_DIR = /home/wenqi/.sdkman
2020-03-19 16:38:09,830 1213 [main] INFO  io.pravega.controller.util.Config - os.arch = amd64
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - java.class.path = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-standalone-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-server-host-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-controller-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-client-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-server-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-storage-impl-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-bindings-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-storage-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-segmentstore-contracts-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-protocol-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-controller-api-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-metrics-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-security-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-cluster-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-common-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/pravega-shared-authplugin-0.6.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-minicluster-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-common-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/logback-classic-1.2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-recipes-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-auth-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-framework-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-tests-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-resourcemanager-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-client-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-server-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/object-client-3.0.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/curator-test-4.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-jobclient-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-jobclient-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-hs-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-app-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-shuffle-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-nodemanager-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-web-proxy-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-applicationhistoryservice-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-server-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-3.5.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-3.5.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-mapreduce-client-core-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/avro-1.7.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-registry-influx-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-common-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-proto-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/http-server-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/circe-checksum-4.7.3.nar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/smart-client-2.1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/object-transform-1.1.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-client-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-common-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-jersey2-jaxrs-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-jaxrs-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-core-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/bookkeeper-stats-api-4.7.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/apacheds-i18n-2.0.0-M15.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/api-asn1-api-1.0.0-M20.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/api-util-1.0.0-M20.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-models-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/slf4j-api-1.7.25.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-container-grizzly2-http-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-container-servlet-core-2.25.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.ws.rs-api-2.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-2.8.5-tests.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-tcnative-boringssl-static-2.0.17.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-io-2.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-yarn-api-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-protobuf-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-netty-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-stub-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-auth-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-protobuf-lite-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-core-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guava-28.1-jre.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-native-epoll-4.1.30.Final-linux-x86_64.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.servlet-api-4.0.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-lang3-3.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-hk2-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-json-jackson-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-runtime-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-core-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-json-1.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-impl-2.2.3-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jaxb-api-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/rocksdbjni-5.13.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-server-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-multipart-2.25.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-client-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-media-jaxb-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-common-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-locator-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-api-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hk2-utils-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.inject-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-http-server-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-entity-filtering-2.29.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.ws.rs-api-2.1.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-annotations-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-cli-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-math3-3.1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/xmlenc-0.52.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jets3t-0.9.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-apache-client4-1.19.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/httpclient-4.5.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-codec-1.10.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-net-3.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-collections-3.2.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/servlet-api-2.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-sslengine-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jetty-util-6.1.26.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsp-api-2.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-configuration-1.10.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-logging-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/log4j-1.2.17.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-lang-2.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-jaxrs-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-xc-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-mapper-asl-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-core-asl-1.9.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/protobuf-java-3.5.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/gson-2.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsch-0.1.54.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jsr305-3.0.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/htrace-core4-4.0.1-incubating.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-compress-1.18.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/hadoop-hdfs-client-2.8.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-daemon-1.0.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-3.10.5.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-all-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/xercesImpl-2.9.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/leveldbjni-all-1.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/logback-core-1.2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/failureaccess-1.0.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/checker-qual-2.8.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/error_prone_annotations-2.3.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/j2objc-annotations-1.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/animal-sniffer-annotations-1.18.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-http2-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-handler-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-native-unix-common-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-handler-proxy-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-http-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-socks-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-codec-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-transport-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-buffer-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-resolver-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/netty-common-4.1.30.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-registry-statsd-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/micrometer-core-1.2.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jjwt-0.9.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-module-jaxb-annotations-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-databind-2.9.9.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-annotations-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/stax-ex-1.7.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/FastInfoset-1.2.13.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/commons-collections4-4.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jcommander-1.48.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jna-3.2.7.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jdom2-2.0.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-http-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jakarta.annotation-api-1.3.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/osgi-resource-locator-1.0.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/validation-api-2.0.1.Final.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/zookeeper-jute-3.5.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/audience-annotations-0.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/httpcore-4.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jettison-1.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/java-xmlbuilder-0.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/paranamer-2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/snappy-java-1.0.4.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/nimbus-jose-jwt-4.41.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/okhttp-2.4.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-guice-1.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guice-servlet-3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/proto-google-common-protos-1.0.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/google-auth-library-credentials-0.9.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/HdrHistogram-2.1.11.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/LatencyUtils-2.0.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/reflections-0.9.11.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.inject-2.5.0-b32.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/mimepull-1.9.6.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/aopalliance-repackaged-2.5.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javassist-3.22.0-CR2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-dataformat-yaml-2.9.8.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jackson-core-2.9.9.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/txw2-2.3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/istack-commons-runtime-3.0.5.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jersey-client-1.19.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/lzma-sdk-4j-9.22.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grizzly-framework-2.4.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/servlet-api-2.5-20081211.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/jcip-annotations-1.0-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/json-smart-2.3.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/okio-1.4.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/guice-3.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/grpc-context-1.17.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/opencensus-contrib-grpc-metrics-0.17.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/opencensus-api-0.17.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/accessors-smart-1.2.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/javax.inject-1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/aopalliance-1.0.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/cglib-2.2.1-v20090111.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/snakeyaml-1.23.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/swagger-annotations-1.5.22.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/asm-5.0.4.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/lib/asm-3.1.jar:/home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/pluginlib/*
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - XDG_SEAT = seat0
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - java.vm.version = 25.181-b13
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - JAVA = /home/wenqi/Desktop/java/src/github.com
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - sun.cpu.endian = little
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - XDG_VTNR = 1
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - SDKMAN_PLATFORM = Linux64
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_AUTHORS = Tom Kaitchuck <Tom.Kaitchuck@dell.com>:Wenqi Mou <wenqi.mou@dell.com>:Sandeep Shridhar <sandeep.shridhar@dell.com>:Wenxiao Zhang <wenxiao.zhang@dell.com>
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - user.dir = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test
2020-03-19 16:38:09,831 1214 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_VERSION_PATCH = 0
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - SSL_CERT_DIR = /usr/lib/ssl/certs
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - IM_CONFIG_PHASE = 2
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - RUSTUP_TOOLCHAIN = stable-x86_64-unknown-linux-gnu
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - java.vm.vendor = Oracle Corporation
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - _ = /home/wenqi/.cargo/bin/cargo
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - user.home = /home/wenqi
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - USERNAME = wenqi
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - os.version = 4.15.0-91-generic
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - java.ext.dirs = /usr/java/jdk1.8.0_181/jre/lib/ext:/usr/java/packages/lib/ext
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - SSL_CERT_FILE = /usr/lib/ssl/certs/ca-certificates.crt
2020-03-19 16:38:09,832 1215 [main] INFO  io.pravega.controller.util.Config - sun.cpu.isalist = 
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_NAME = pravega-rust-client-integration-test
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - awt.toolkit = sun.awt.X11.XToolkit
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - SHELL = /bin/bash
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_VERSION_MAJOR = 0
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - java.vm.specification.version = 1.8
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - JAVA_HOME = /usr/java/jdk1.8.0_181
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - OLDPWD = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - GTK_IM_MODULE = ibus
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - java.awt.graphicsenv = sun.awt.X11GraphicsEnvironment
2020-03-19 16:38:09,833 1216 [main] INFO  io.pravega.controller.util.Config - AWS = /home/wenqi/Desktop/go/src/github.com/cr/aws
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - TEXTDOMAINDIR = /usr/share/locale/
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - sun.management.compiler = HotSpot 64-Bit Tiered Compilers
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - logback.configurationFile = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/conf/logback.xml
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - sun.boot.library.path = /usr/java/jdk1.8.0_181/jre/lib/amd64
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - GNOME_SHELL_SESSION_MODE = ubuntu
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - OPEN = /home/wenqi/Desktop/go/src/github.com/cr/open
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - zookeeper.4lw.commands.whitelist = *
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - DBUS_SESSION_BUS_ADDRESS = unix:path=/run/user/1000/bus
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_DESCRIPTION = The integration test for pravega rust client.
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - java.class.version = 52.0
2020-03-19 16:38:09,834 1217 [main] INFO  io.pravega.controller.util.Config - QT_IM_MODULE = xim
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - GRADLE_HOME = /home/wenqi/.sdkman/candidates/gradle/current
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - RUST = /home/wenqi/Desktop/rust/projects
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - GJS_DEBUG_TOPICS = JS ERROR;JS LOG
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - java.library.path = /home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/build/backtrace-sys-b28453e14347046d/out:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug/deps:/home/wenqi/Desktop/rust/projects/pravega-client-rust/target/debug:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib:/home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - java.vm.name = Java HotSpot(TM) 64-Bit Server VM
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - java.specification.vendor = Oracle Corporation
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - XDG_CURRENT_DESKTOP = ubuntu:GNOME
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - os.name = Linux
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - JARVIS_USERNAME = wenqi.mou
2020-03-19 16:38:09,835 1218 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_VERSION = 0.1.0
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - XDG_MENU_PREFIX = gnome-
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - LESSCLOSE = /usr/bin/lesspipe %s %s
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - SDKMAN_VERSION = 5.7.3+337
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - CARGO = /home/wenqi/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/cargo
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - XAUTHORITY = /run/user/1000/gdm/Xauthority
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - GNOME_DESKTOP_SESSION_ID = this-is-deprecated
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - java.awt.printerjob = sun.print.PSPrinterJob
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - SESSION_MANAGER = local/wenqi-VirtualBox:@/tmp/.ICE-unix/1312,unix/wenqi-VirtualBox:/tmp/.ICE-unix/1312
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - CARGO_MANIFEST_DIR = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - DEFAULTS_PATH = /usr/share/gconf/ubuntu.default.path
2020-03-19 16:38:09,836 1219 [main] INFO  io.pravega.controller.util.Config - GNOME_TERMINAL_SERVICE = :1.68
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - JRE_HOME = /usr/java/jdk1.8.0_181/jre
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_REPOSITORY = https://github.com/pravega/pravega-client-rust
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - GJS_DEBUG_OUTPUT = stderr
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - idea = /home/Downloads/idea-IC-192.6817.14/bin/idea.sh
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - CARGO_HOME = /home/wenqi/.cargo
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_VERSION_MINOR = 1
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - WINDOWPATH = 1
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_HOMEPAGE = 
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - java.vm.specification.name = Java Virtual Machine Specification
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - GOBIN = /home/wenqi/Desktop/go/bin
2020-03-19 16:38:09,837 1220 [main] INFO  io.pravega.controller.util.Config - sun.desktop = gnome
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - java.vm.info = mixed mode
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - USER = wenqi
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - log.dir = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/logs
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - SHLVL = 1
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - SSH_AGENT_PID = 1495
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - COLORTERM = truecolor
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - line.separator = 

2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - GOPATH = /home/wenqi/Desktop/go
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - user.language = en
2020-03-19 16:38:09,838 1221 [main] INFO  io.pravega.controller.util.Config - java.io.tmpdir = /tmp
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - singlenode.configurationFile = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test/pravega/conf/standalone-config.properties
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - XDG_SESSION_TYPE = x11
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - CARGO_PKG_VERSION_PRE = 
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - sun.java.command = io.pravega.local.LocalPravegaEmulator
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - RUST_RECURSION_COUNT = 1
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - XDG_CONFIG_DIRS = /etc/xdg/xdg-ubuntu:/etc/xdg
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - DESKTOP_SESSION = ubuntu
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - QT_ACCESSIBILITY = 1
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - PATH = /home/wenqi/.cargo/bin:/home/wenqi/.cargo/bin:/home/wenqi/.sdkman/candidates/gradle/current/bin:/home/wenqi/.cargo/bin:/home/wenqi/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/java/jdk1.8.0_181/bin:/usr/java/jdk1.8.0_181/jre/bin:/usr/local/go/bin:/home/wenqi/Desktop/go/bin:/home/wenqi/.local/bin:/home/Desktop/fast
2020-03-19 16:38:09,839 1222 [main] INFO  io.pravega.controller.util.Config - file.encoding = UTF-8
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - GTK_MODULES = gail:atk-bridge
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - MANDATORY_PATH = /usr/share/gconf/ubuntu.mandatory.path
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - TERM = xterm-256color
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - path.separator = :
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - user.timezone = America/Los_Angeles
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - XDG_SESSION_DESKTOP = ubuntu
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - java.specification.name = Java Platform API Specification
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - SDKMAN_CANDIDATES_DIR = /home/wenqi/.sdkman/candidates
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - java.runtime.name = Java(TM) SE Runtime Environment
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - sun.jnu.encoding = UTF-8
2020-03-19 16:38:09,840 1223 [main] INFO  io.pravega.controller.util.Config - SDKMAN_CANDIDATES_API = https://api.sdkman.io/2
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - sun.io.unicode.encoding = UnicodeLittle
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - XDG_RUNTIME_DIR = /run/user/1000
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - user.name = wenqi
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - XDG_DATA_DIRS = /usr/share/ubuntu:/usr/local/share:/usr/share:/var/lib/snapd/desktop
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - java.vendor.url.bug = http://bugreport.sun.com/bugreport/
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - TEXTDOMAIN = im-config
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - GPG_AGENT_INFO = /run/user/1000/gnupg/S.gpg-agent:0:1
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - java.version = 1.8.0_181
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - DOCKER_REPOSITORY = tristan1900
2020-03-19 16:38:09,841 1224 [main] INFO  io.pravega.controller.util.Config - java.vendor.url = http://java.oracle.com/
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - sun.os.patch.level = unknown
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - java.endorsed.dirs = /usr/java/jdk1.8.0_181/jre/lib/endorsed
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - jdk.tls.rejectClientInitiatedRenegotiation = true
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - GDMSESSION = ubuntu
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - PWD = /home/wenqi/Desktop/rust/projects/pravega-client-rust/integration_test
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - VTE_VERSION = 5202
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - LOGNAME = wenqi
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - java.home = /usr/java/jdk1.8.0_181/jre
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - CLUTTER_IM_MODULE = xim
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - file.encoding.pkg = sun.io
2020-03-19 16:38:09,842 1225 [main] INFO  io.pravega.controller.util.Config - DISPLAY = :0
2020-03-19 16:38:09,843 1226 [main] INFO  io.pravega.controller.util.Config - publishedRPCHost is not configured, will use default value
2020-03-19 16:38:09,886 1269 [ControllerServiceMain] INFO  i.p.c.server.ControllerServiceMain - Creating store client
2020-03-19 16:38:09,892 1275 [ControllerServiceMain] WARN  o.a.curator.CuratorZookeeperClient - session timeout [10000] is less than connection timeout [15000]
2020-03-19 16:38:09,893 1276 [ControllerServiceMain] INFO  o.a.c.f.imps.CuratorFrameworkImpl - Starting
2020-03-19 16:38:09,894 1277 [ControllerServiceMain] INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=localhost:4000 sessionTimeout=10000 watcher=org.apache.curator.ConnectionState@760f444c
2020-03-19 16:38:09,895 1278 [ControllerServiceMain] INFO  o.apache.zookeeper.ClientCnxnSocket - jute.maxbuffer value is 4194304 Bytes
2020-03-19 16:38:09,895 1278 [ControllerServiceMain] INFO  org.apache.zookeeper.ClientCnxn - zookeeper.request.timeout value is 0. feature enabled=
2020-03-19 16:38:09,896 1279 [ControllerServiceMain-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server localhost/127.0.0.1:4000. Will not attempt to authenticate using SASL (unknown error)
2020-03-19 16:38:09,897 1280 [ControllerServiceMain-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Socket connection established, initiating session, client: /127.0.0.1:51244, server: localhost/127.0.0.1:4000
2020-03-19 16:38:09,898 1281 [ControllerServiceMain] INFO  o.a.c.f.imps.CuratorFrameworkImpl - Default schema
2020-03-19 16:38:09,901 1284 [ControllerServiceMain-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Session establishment complete on server localhost/127.0.0.1:4000, sessionid = 0x10006911fc30001, negotiated timeout = 10000
2020-03-19 16:38:09,901 1284 [ControllerServiceMain-EventThread] INFO  o.a.c.f.s.ConnectionStateManager - State change: CONNECTED
2020-03-19 16:38:09,901 1284 [ControllerServiceMain] INFO  i.p.c.server.ControllerServiceMain - Awaiting ZK client connection to ZK server
2020-03-19 16:38:09,902 1285 [ControllerServiceMain] INFO  i.p.c.server.ControllerServiceMain - Awaiting ZK session expiry or termination trigger for ControllerServiceMain
2020-03-19 16:38:09,903 1286 [ControllerServiceMain-EventThread] INFO  o.a.c.f.imps.EnsembleTracker - New config event received: {}
2020-03-19 16:38:09,905 1288 [ControllerServiceMain] INFO  i.p.c.server.ControllerServiceMain - Starting controller services
2020-03-19 16:38:09,906 1289 [ControllerServiceMain-EventThread] INFO  o.a.c.f.imps.EnsembleTracker - New config event received: {}
2020-03-19 16:38:09,907 1290 [ControllerServiceMain] INFO  i.p.c.server.ControllerServiceMain - Awaiting controller services start
2020-03-19 16:38:09,908 1291 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Initiating controller service startUp
2020-03-19 16:38:09,910 1293 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Controller serviceConfig = ControllerServiceConfigImpl(threadPoolSize=80, storeClientConfig=StoreClientConfigImpl(storeType=Zookeeper, zkClientConfig=Optional[ZKClientConfigImpl(connectionString: localhost:4000, namespace: pravega/singlenode-920a7ec6-900a-4a52-bd54-e789db820c61, initialSleepInterval: 2000, maxRetries: 1, sessionTimeoutMs: 10000, secureConnectionToZooKeeper: false, trustStorePath is unspecified, trustStorePasswordPath is unspecified)]), hostMonitorConfig=io.pravega.controller.store.host.impl.HostMonitorConfigImpl@12d4dc38, controllerClusterListenerEnabled=false, timeoutServiceConfig=TimeoutServiceConfig(maxLeaseValue=120000), tlsEnabledForSegmentStore=null, eventProcessorConfig=Optional[ControllerEventProcessorConfigImpl(scopeName=_system, commitStreamName=_commitStream, commitStreamScalingPolicy=ScalingPolicy(scaleType=FIXED_NUM_SEGMENTS, targetRate=0, scaleFactor=0, minNumSegments=2), abortStreamName=_abortStream, abortStreamScalingPolicy=ScalingPolicy(scaleType=FIXED_NUM_SEGMENTS, targetRate=0, scaleFactor=0, minNumSegments=2), scaleStreamName=_requeststream, scaleStreamScalingPolicy=ScalingPolicy(scaleType=FIXED_NUM_SEGMENTS, targetRate=0, scaleFactor=0, minNumSegments=2), commitReaderGroupName=commitStreamReaders, commitReaderGroupSize=1, abortReaderGroupName=abortStreamReaders, abortReaderGroupSize=1, scaleReaderGroupName=scaleGroup, scaleReaderGroupSize=1, commitCheckpointConfig=CheckpointConfig(type=Periodic, checkpointPeriod=CheckpointConfig.CheckpointPeriod(numEvents=10, numSeconds=10)), abortCheckpointConfig=CheckpointConfig(type=Periodic, checkpointPeriod=CheckpointConfig.CheckpointPeriod(numEvents=10, numSeconds=10)), scaleCheckpointConfig=CheckpointConfig(type=None, checkpointPeriod=null))], gRPCServerConfig=Optional[GRPCServerConfigImpl(port: 9090, publishedRPCHost: localhost, publishedRPCPort: 9090, authorizationEnabled: false, userPasswordFile is unspecified, tokenSigningKey is specified, accessTokenTTLInSeconds: 600, tlsEnabled: false, tlsCertFile is unspecified, tlsKeyFile is unspecified, tlsTrustStore is unspecified, replyWithStackTraceOnError: false, requestTracingEnabled: true)], restServerConfig=Optional[RESTServerConfigImpl(host: 0.0.0.0, port: 9091, tlsEnabled: false, keyFilePath is unspecified, keyFilePasswordPath is unspecified)])
2020-03-19 16:38:09,911 1294 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Event processors enabled = true
2020-03-19 16:38:09,911 1294 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Cluster listener enabled = false
2020-03-19 16:38:09,911 1294 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter -     Host monitor enabled = true
2020-03-19 16:38:09,911 1294 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter -      gRPC server enabled = true
2020-03-19 16:38:09,911 1294 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter -      REST server enabled = true
2020-03-19 16:38:09,912 1295 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Creating the bucket store
2020-03-19 16:38:09,929 1312 [main] INFO  i.p.s.server.host.ServiceStarter - Initializing metrics provider ...
2020-03-19 16:38:09,930 1313 [main] INFO  i.p.s.metrics.StatsProviderProxy - Stats disabled
2020-03-19 16:38:09,934 1317 [main] INFO  i.p.s.server.host.ServiceStarter - Initializing ZooKeeper Client ...
2020-03-19 16:38:09,935 1318 [main] WARN  o.a.curator.CuratorZookeeperClient - session timeout [10000] is less than connection timeout [15000]
2020-03-19 16:38:09,936 1319 [main] INFO  o.a.c.f.imps.CuratorFrameworkImpl - Starting
2020-03-19 16:38:09,936 1319 [main] INFO  i.p.s.server.host.ServiceStarter - Creating new Zookeeper client with arguments: localhost:4000, 10000, false.
2020-03-19 16:38:09,936 1319 [main] INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=localhost:4000 sessionTimeout=10000 watcher=org.apache.curator.ConnectionState@4bff1903
2020-03-19 16:38:09,937 1320 [main] INFO  o.apache.zookeeper.ClientCnxnSocket - jute.maxbuffer value is 4194304 Bytes
2020-03-19 16:38:09,937 1320 [main] INFO  org.apache.zookeeper.ClientCnxn - zookeeper.request.timeout value is 0. feature enabled=
2020-03-19 16:38:09,938 1321 [main-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server localhost/127.0.0.1:4000. Will not attempt to authenticate using SASL (unknown error)
2020-03-19 16:38:09,938 1321 [main-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Socket connection established, initiating session, client: /127.0.0.1:51246, server: localhost/127.0.0.1:4000
2020-03-19 16:38:09,941 1324 [main-SendThread(localhost:4000)] INFO  org.apache.zookeeper.ClientCnxn - Session establishment complete on server localhost/127.0.0.1:4000, sessionid = 0x10006911fc30002, negotiated timeout = 10000
2020-03-19 16:38:09,947 1330 [main-EventThread] INFO  o.a.c.f.s.ConnectionStateManager - State change: CONNECTED
2020-03-19 16:38:09,948 1331 [main] INFO  o.a.c.f.imps.CuratorFrameworkImpl - Default schema
2020-03-19 16:38:09,948 1331 [main] INFO  i.p.s.server.host.ServiceStarter - Initializing Service Builder ...
2020-03-19 16:38:09,954 1337 [main-EventThread] INFO  o.a.c.f.imps.EnsembleTracker - New config event received: {}
2020-03-19 16:38:09,955 1338 [main-EventThread] INFO  o.a.c.f.imps.EnsembleTracker - New config event received: {}
2020-03-19 16:38:09,962 1345 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Creating the task store
2020-03-19 16:38:09,968 1351 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Creating the host store
2020-03-19 16:38:09,974 1357 [ControllerServiceStarter STARTING] INFO  i.p.c.store.host.HostStoreFactory - Creating Zookeeper based host store
2020-03-19 16:38:10,007 1390 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Creating the checkpoint store
2020-03-19 16:38:10,042 1425 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Starting segment container monitor
2020-03-19 16:38:10,101 1484 [main] INFO  i.p.s.s.i.r.RocksDBCacheFactory - RocksDBCacheFactory: Initialized.
2020-03-19 16:38:10,106 1489 [Curator-LeaderSelector-0] INFO  i.p.c.fault.SegmentMonitorLeader - Obtained leadership to monitor the Host to Segment Container Mapping
2020-03-19 16:38:10,109 1492 [main] INFO  i.p.s.server.host.StorageLoader - Loading INMEMORY, trying FILESYSTEM
2020-03-19 16:38:10,110 1493 [main] INFO  i.p.s.server.host.StorageLoader - Loading INMEMORY, trying HDFS
2020-03-19 16:38:10,110 1493 [main] INFO  i.p.s.server.host.StorageLoader - Loading INMEMORY, trying EXTENDEDS3
2020-03-19 16:38:10,111 1494 [main] INFO  i.p.s.server.host.StorageLoader - Loading INMEMORY, trying INMEMORY
2020-03-19 16:38:10,131 1514 [Curator-LeaderSelector-0] INFO  i.p.c.fault.SegmentMonitorLeader - Received rebalance event
2020-03-19 16:38:10,132 1515 [Curator-LeaderSelector-0] INFO  i.p.c.fault.SegmentMonitorLeader - Waiting for 10 seconds before attempting to rebalance
2020-03-19 16:38:10,145 1528 [core-2] WARN  i.p.s.s.h.ZKSegmentContainerMonitor - No segment container assignments found
2020-03-19 16:38:10,167 1550 [main] INFO  i.p.s.s.h.ZKSegmentContainerManager - Initialized.
2020-03-19 16:38:10,168 1551 [main] INFO  i.p.s.server.host.ServiceStarter - Creating StreamSegmentService ...
2020-03-19 16:38:10,171 1554 [main] INFO  i.p.s.server.host.ServiceStarter - Creating TableStoreService ...
2020-03-19 16:38:10,173 1556 [main] INFO  i.p.s.server.host.ServiceStarter - Creating Segment Stats recorder ...
2020-03-19 16:38:10,184 1567 [Curator-PathChildrenCache-0] INFO  i.p.c.cluster.zkImpl.ClusterZKImpl - Node 127.0.1.1:6000: added to cluster
2020-03-19 16:38:10,188 1571 [Curator-PathChildrenCache-0] INFO  i.p.c.fault.SegmentMonitorLeader - Received event: HOST_ADDED for host: 127.0.1.1:6000:. Wake up leader for rebalancing
2020-03-19 16:38:10,334 1717 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Creating the stream store
2020-03-19 16:38:10,504 1887 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - starting background periodic service for retention
2020-03-19 16:38:10,554 1937 [controllerpool-2] INFO  i.p.c.server.bucket.BucketManager - RetentionService: Taken ownership for bucket 0
2020-03-19 16:38:10,565 1948 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 0 change notification listener registered
2020-03-19 16:38:10,567 1950 [retentionpool-1] INFO  i.p.c.server.bucket.BucketManager - RetentionService: successfully started bucket service bucket: 0 
2020-03-19 16:38:10,568 1951 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - RetentionService: Notification loop started for bucket 0
2020-03-19 16:38:10,570 1953 [retentionpool-1] INFO  i.p.c.s.b.ZooKeeperBucketManager - bucket ownership listener registered on bucket root RetentionService
2020-03-19 16:38:10,571 1954 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - RetentionService: Notification loop started for bucket 0
2020-03-19 16:38:10,572 1955 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - RetentionService: bucket 0 service start completed
2020-03-19 16:38:10,582 1965 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - starting background periodic service for watermarking
2020-03-19 16:38:10,708 2091 [main] INFO  i.p.c.stream.impl.ControllerImpl - Controller client connecting to server at localhost:9090
2020-03-19 16:38:10,758 2141 [main] INFO  i.p.s.server.host.ServiceStarter - ServiceConfig(containerCount: 4, coreThreadPoolSize: 20, storageThreadPoolSize: 200, listeningPort: 6000, listeningIPAddress: 127.0.1.1, publishedPort: 6000, publishedIPAddress: 127.0.1.1, zkURL: localhost:4000, zkRetrySleepMs: 5000, zkSessionTimeoutMs: 10000, zkRetryCount: 5, secureZK: false, zkTrustStore is unspecified, zkTrustStorePasswordPath is unspecified, clusterName: singlenode-920a7ec6-900a-4a52-bd54-e789db820c61, dataLogTypeImplementation: INMEMORY, storageImplementation: INMEMORY, readOnlySegmentStore: false, enableTls: false, certFile is unspecified, keyFile is unspecified, enableTlsReload: false, cachePolicy is MaxSize = 134217728, UsableSize = 114085068, MaxGen = 60, Generation = PT1S, replyWithStackTraceOnError: false, instanceId: )
2020-03-19 16:38:10,761 2144 [main] INFO  i.p.s.server.host.ServiceStarter - AutoScalerConfig(controllerUri: tcp://localhost:9090, internalRequestStream: _requeststream, cooldownDuration: PT10M, muteDuration: PT10M, cacheExpiry: PT20M, cacheCleanup: PT5M, tlsEnabled: false, tlsCertFile is unspecified, authEnabled: false, tokenSigningKey is specified, validateHostName: false, threadPoolSize: 10)
2020-03-19 16:38:10,805 2188 [controllerpool-46] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 0
2020-03-19 16:38:10,806 2189 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 0 change notification listener registered
2020-03-19 16:38:10,807 2190 [controllerpool-14] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 1
2020-03-19 16:38:10,822 2205 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 0 
2020-03-19 16:38:10,834 2217 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 0
2020-03-19 16:38:10,834 2217 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 0
2020-03-19 16:38:10,834 2217 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 0 service start completed
2020-03-19 16:38:10,835 2218 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 1 change notification listener registered
2020-03-19 16:38:10,838 2221 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 1 
2020-03-19 16:38:10,839 2222 [controllerpool-72] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 2
2020-03-19 16:38:10,846 2229 [controllerpool-25] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 3
2020-03-19 16:38:10,852 2235 [controllerpool-36] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 4
2020-03-19 16:38:10,866 2249 [controllerpool-13] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 5
2020-03-19 16:38:10,866 2249 [controllerpool-47] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 6
2020-03-19 16:38:10,867 2250 [controllerpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 7
2020-03-19 16:38:10,867 2250 [controllerpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 8
2020-03-19 16:38:10,867 2250 [controllerpool-5] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 9
2020-03-19 16:38:10,867 2250 [controllerpool-79] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 10
2020-03-19 16:38:10,867 2250 [controllerpool-77] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 11
2020-03-19 16:38:10,867 2250 [controllerpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 13
2020-03-19 16:38:10,868 2251 [controllerpool-15] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 14
2020-03-19 16:38:10,868 2251 [controllerpool-46] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 12
2020-03-19 16:38:10,868 2251 [controllerpool-48] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 15
2020-03-19 16:38:10,868 2251 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 1
2020-03-19 16:38:10,870 2253 [controllerpool-16] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 16
2020-03-19 16:38:10,870 2253 [controllerpool-50] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 18
2020-03-19 16:38:10,871 2254 [controllerpool-53] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 19
2020-03-19 16:38:10,871 2254 [controllerpool-17] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 21
2020-03-19 16:38:10,871 2254 [controllerpool-52] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 20
2020-03-19 16:38:10,871 2254 [controllerpool-55] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 22
2020-03-19 16:38:10,872 2255 [controllerpool-18] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 23
2020-03-19 16:38:10,872 2255 [controllerpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 24
2020-03-19 16:38:10,872 2255 [controllerpool-56] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 25
2020-03-19 16:38:10,872 2255 [controllerpool-57] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 26
2020-03-19 16:38:10,873 2256 [controllerpool-63] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 28
2020-03-19 16:38:10,873 2256 [controllerpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 29
2020-03-19 16:38:10,874 2257 [controllerpool-62] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 30
2020-03-19 16:38:10,874 2257 [controllerpool-59] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 31
2020-03-19 16:38:10,874 2257 [controllerpool-61] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 32
2020-03-19 16:38:10,875 2258 [controllerpool-19] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 33
2020-03-19 16:38:10,875 2258 [controllerpool-65] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 17
2020-03-19 16:38:10,876 2259 [controllerpool-64] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 34
2020-03-19 16:38:10,876 2259 [controllerpool-67] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 35
2020-03-19 16:38:10,877 2260 [controllerpool-68] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 37
2020-03-19 16:38:10,877 2260 [controllerpool-20] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 38
2020-03-19 16:38:10,877 2260 [controllerpool-58] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 40
2020-03-19 16:38:10,877 2260 [controllerpool-11] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 39
2020-03-19 16:38:10,877 2260 [controllerpool-73] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 42
2020-03-19 16:38:10,878 2261 [controllerpool-70] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 27
2020-03-19 16:38:10,878 2261 [controllerpool-71] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 41
2020-03-19 16:38:10,879 2262 [controllerpool-74] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 43
2020-03-19 16:38:10,879 2262 [controllerpool-75] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 44
2020-03-19 16:38:10,879 2262 [controllerpool-76] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 45
2020-03-19 16:38:10,880 2263 [controllerpool-69] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 46
2020-03-19 16:38:10,880 2263 [controllerpool-4] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 47
2020-03-19 16:38:10,880 2263 [controllerpool-24] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 48
2020-03-19 16:38:10,880 2263 [controllerpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 49
2020-03-19 16:38:10,880 2263 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 1
2020-03-19 16:38:10,880 2263 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 1 service start completed
2020-03-19 16:38:10,880 2263 [controllerpool-23] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 51
2020-03-19 16:38:10,881 2264 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 4 change notification listener registered
2020-03-19 16:38:10,882 2265 [controllerpool-22] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 50
2020-03-19 16:38:10,884 2267 [controllerpool-33] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 56
2020-03-19 16:38:10,883 2266 [controllerpool-28] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 54
2020-03-19 16:38:10,886 2269 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 3 change notification listener registered
2020-03-19 16:38:10,887 2270 [watermarkingpool-5] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 4 
2020-03-19 16:38:10,884 2267 [controllerpool-30] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 55
2020-03-19 16:38:10,883 2266 [controllerpool-21] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 52
2020-03-19 16:38:10,882 2265 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 4
2020-03-19 16:38:10,886 2269 [controllerpool-32] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 57
2020-03-19 16:38:10,888 2271 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 4
2020-03-19 16:38:10,883 2266 [controllerpool-27] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 53
2020-03-19 16:38:10,888 2271 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 4 service start completed
2020-03-19 16:38:10,889 2272 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 2 change notification listener registered
2020-03-19 16:38:10,893 2276 [controllerpool-29] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 58
2020-03-19 16:38:10,894 2277 [controllerpool-31] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 60
2020-03-19 16:38:10,894 2277 [controllerpool-54] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 59
2020-03-19 16:38:10,896 2279 [controllerpool-35] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 61
2020-03-19 16:38:10,896 2279 [controllerpool-34] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 62
2020-03-19 16:38:10,897 2280 [controllerpool-37] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 63
2020-03-19 16:38:10,897 2280 [controllerpool-60] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 65
2020-03-19 16:38:10,898 2281 [controllerpool-38] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 66
2020-03-19 16:38:10,898 2281 [controllerpool-78] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 69
2020-03-19 16:38:10,898 2281 [controllerpool-39] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 68
2020-03-19 16:38:10,899 2282 [controllerpool-14] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 70
2020-03-19 16:38:10,900 2283 [controllerpool-41] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 67
2020-03-19 16:38:10,901 2284 [controllerpool-80] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 73
2020-03-19 16:38:10,906 2289 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 70 change notification listener registered
2020-03-19 16:38:10,910 2293 [controllerpool-42] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 64
2020-03-19 16:38:10,911 2294 [controllerpool-40] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 71
2020-03-19 16:38:10,911 2294 [controllerpool-42] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 72
2020-03-19 16:38:10,911 2294 [controllerpool-40] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 74
2020-03-19 16:38:10,911 2294 [controllerpool-42] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 77
2020-03-19 16:38:10,911 2294 [controllerpool-40] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 78
2020-03-19 16:38:10,911 2294 [controllerpool-42] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 76
2020-03-19 16:38:10,911 2294 [controllerpool-40] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 80
2020-03-19 16:38:10,912 2295 [controllerpool-40] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 82
2020-03-19 16:38:10,912 2295 [controllerpool-27] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 75
2020-03-19 16:38:10,912 2295 [controllerpool-42] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 81
2020-03-19 16:38:10,912 2295 [controllerpool-40] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 79
2020-03-19 16:38:10,912 2295 [controllerpool-32] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 83
2020-03-19 16:38:10,912 2295 [controllerpool-43] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 85
2020-03-19 16:38:10,912 2295 [controllerpool-44] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 84
2020-03-19 16:38:10,913 2296 [controllerpool-45] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 87
2020-03-19 16:38:10,913 2296 [controllerpool-21] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 90
2020-03-19 16:38:10,912 2295 [controllerpool-42] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 36
2020-03-19 16:38:10,913 2296 [controllerpool-30] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 89
2020-03-19 16:38:10,913 2296 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 3
2020-03-19 16:38:10,912 2295 [controllerpool-32] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 86
2020-03-19 16:38:10,913 2296 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 3
2020-03-19 16:38:10,913 2296 [controllerpool-43] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 88
2020-03-19 16:38:10,913 2296 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 3 service start completed
2020-03-19 16:38:10,914 2297 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 63 change notification listener registered
2020-03-19 16:38:10,915 2298 [controllerpool-75] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 91
2020-03-19 16:38:10,916 2299 [controllerpool-34] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 92
2020-03-19 16:38:10,916 2299 [controllerpool-35] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 93
2020-03-19 16:38:10,917 2300 [controllerpool-58] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 96
2020-03-19 16:38:10,918 2301 [controllerpool-80] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 95
2020-03-19 16:38:10,919 2302 [controllerpool-31] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 94
2020-03-19 16:38:10,919 2302 [controllerpool-62] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 97
2020-03-19 16:38:10,919 2302 [controllerpool-21] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 98
2020-03-19 16:38:10,920 2303 [controllerpool-12] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: Taken ownership for bucket 99
2020-03-19 16:38:10,921 2304 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 2
2020-03-19 16:38:10,922 2305 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 2
2020-03-19 16:38:10,922 2305 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 2 service start completed
2020-03-19 16:38:10,922 2305 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 68 change notification listener registered
2020-03-19 16:38:10,932 2315 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 70
2020-03-19 16:38:10,932 2315 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 70
2020-03-19 16:38:10,932 2315 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 70 service start completed
2020-03-19 16:38:10,933 2316 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 69 change notification listener registered
2020-03-19 16:38:10,933 2316 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 3 
2020-03-19 16:38:10,935 2318 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 70 
2020-03-19 16:38:10,932 2315 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 2 
2020-03-19 16:38:10,950 2333 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 63
2020-03-19 16:38:10,950 2333 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 63
2020-03-19 16:38:10,950 2333 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 63 service start completed
2020-03-19 16:38:10,951 2334 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 62 change notification listener registered
2020-03-19 16:38:10,951 2334 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 63 
2020-03-19 16:38:10,954 2337 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 68
2020-03-19 16:38:10,954 2337 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 68
2020-03-19 16:38:10,955 2338 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 68 service start completed
2020-03-19 16:38:10,955 2338 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 61 change notification listener registered
2020-03-19 16:38:10,955 2338 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 68 
2020-03-19 16:38:10,966 2349 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 69
2020-03-19 16:38:10,966 2349 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 69 
2020-03-19 16:38:10,966 2349 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 69
2020-03-19 16:38:10,986 2369 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 62
2020-03-19 16:38:10,986 2369 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 62
2020-03-19 16:38:10,987 2370 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 62 service start completed
2020-03-19 16:38:10,987 2370 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 71 change notification listener registered
2020-03-19 16:38:10,987 2370 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 62 
2020-03-19 16:38:10,993 2376 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 69 service start completed
2020-03-19 16:38:10,993 2376 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 61 
2020-03-19 16:38:10,994 2377 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 74 change notification listener registered
2020-03-19 16:38:10,993 2376 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 61
2020-03-19 16:38:10,994 2377 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 61
2020-03-19 16:38:10,994 2377 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 71
2020-03-19 16:38:10,994 2377 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 71
2020-03-19 16:38:10,994 2377 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 61 service start completed
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 71 service start completed
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 59 change notification listener registered
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 66 change notification listener registered
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 74
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 74
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 74 service start completed
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 67 change notification listener registered
2020-03-19 16:38:10,995 2378 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 71 
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 66
2020-03-19 16:38:10,995 2378 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 74 
2020-03-19 16:38:10,995 2378 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 66
2020-03-19 16:38:10,996 2379 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 66 service start completed
2020-03-19 16:38:10,996 2379 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 66 
2020-03-19 16:38:10,996 2379 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 67
2020-03-19 16:38:10,996 2379 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 73 change notification listener registered
2020-03-19 16:38:10,996 2379 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 67
2020-03-19 16:38:10,996 2379 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 67 service start completed
2020-03-19 16:38:10,996 2379 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 67 
2020-03-19 16:38:10,996 2379 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 57 change notification listener registered
2020-03-19 16:38:11,001 2384 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 73
2020-03-19 16:38:11,002 2385 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 73
2020-03-19 16:38:11,002 2385 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 73 service start completed
2020-03-19 16:38:11,001 2384 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 73 
2020-03-19 16:38:11,002 2385 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 59
2020-03-19 16:38:11,002 2385 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 65 change notification listener registered
2020-03-19 16:38:11,002 2385 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 59 
2020-03-19 16:38:11,002 2385 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 59
2020-03-19 16:38:11,002 2385 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 59 service start completed
2020-03-19 16:38:11,003 2386 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 60 change notification listener registered
2020-03-19 16:38:11,003 2386 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 57
2020-03-19 16:38:11,003 2386 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 57
2020-03-19 16:38:11,003 2386 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 57 service start completed
2020-03-19 16:38:11,003 2386 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 78 change notification listener registered
2020-03-19 16:38:11,003 2386 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 57 
2020-03-19 16:38:11,007 2390 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 78 
2020-03-19 16:38:11,007 2390 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 78
2020-03-19 16:38:11,008 2391 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 78
2020-03-19 16:38:11,008 2391 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 78 service start completed
2020-03-19 16:38:11,008 2391 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 83 change notification listener registered
2020-03-19 16:38:11,017 2400 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 60
2020-03-19 16:38:11,017 2400 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 60
2020-03-19 16:38:11,017 2400 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 60 service start completed
2020-03-19 16:38:11,018 2401 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 85 change notification listener registered
2020-03-19 16:38:11,018 2401 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 60 
2020-03-19 16:38:11,022 2405 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 65
2020-03-19 16:38:11,022 2405 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 65
2020-03-19 16:38:11,022 2405 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 65 service start completed
2020-03-19 16:38:11,022 2405 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 52 change notification listener registered
2020-03-19 16:38:11,023 2406 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 65 
2020-03-19 16:38:11,023 2406 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 83 
2020-03-19 16:38:11,023 2406 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 83
2020-03-19 16:38:11,024 2407 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 83
2020-03-19 16:38:11,024 2407 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 83 service start completed
2020-03-19 16:38:11,024 2407 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 80 change notification listener registered
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 85
2020-03-19 16:38:11,029 2412 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 85 
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 85
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 52
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 85 service start completed
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 52
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 52 service start completed
2020-03-19 16:38:11,029 2412 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 52 
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 58 change notification listener registered
2020-03-19 16:38:11,029 2412 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 53 change notification listener registered
2020-03-19 16:38:11,030 2413 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 80
2020-03-19 16:38:11,030 2413 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 80 
2020-03-19 16:38:11,030 2413 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 80
2020-03-19 16:38:11,030 2413 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 80 service start completed
2020-03-19 16:38:11,030 2413 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 86 change notification listener registered
2020-03-19 16:38:11,030 2413 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 58
2020-03-19 16:38:11,030 2413 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 58
2020-03-19 16:38:11,031 2414 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 58 service start completed
2020-03-19 16:38:11,031 2414 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 64 change notification listener registered
2020-03-19 16:38:11,030 2413 [watermarkingpool-4] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 58 
2020-03-19 16:38:11,039 2422 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 53
2020-03-19 16:38:11,039 2422 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 53 
2020-03-19 16:38:11,039 2422 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 53
2020-03-19 16:38:11,039 2422 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 53 service start completed
2020-03-19 16:38:11,040 2423 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 72 change notification listener registered
2020-03-19 16:38:11,054 2437 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 86 
2020-03-19 16:38:11,055 2438 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 64
2020-03-19 16:38:11,055 2438 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 64
2020-03-19 16:38:11,055 2438 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 64 service start completed
2020-03-19 16:38:11,055 2438 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 72
2020-03-19 16:38:11,055 2438 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 72
2020-03-19 16:38:11,055 2438 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 72 service start completed
2020-03-19 16:38:11,056 2439 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 77 change notification listener registered
2020-03-19 16:38:11,056 2439 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 82 change notification listener registered
2020-03-19 16:38:11,058 2441 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 77
2020-03-19 16:38:11,058 2441 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 77
2020-03-19 16:38:11,058 2441 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 77 service start completed
2020-03-19 16:38:11,059 2442 [watermarkingpool-4] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 64 
2020-03-19 16:38:11,059 2442 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 76 change notification listener registered
2020-03-19 16:38:11,058 2441 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 86
2020-03-19 16:38:11,059 2442 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 72 
2020-03-19 16:38:11,059 2442 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 86
2020-03-19 16:38:11,059 2442 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 86 service start completed
2020-03-19 16:38:11,059 2442 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 50 change notification listener registered
2020-03-19 16:38:11,059 2442 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 77 
2020-03-19 16:38:11,062 2445 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 82
2020-03-19 16:38:11,062 2445 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 82
2020-03-19 16:38:11,062 2445 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 82 service start completed
2020-03-19 16:38:11,062 2445 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 55 change notification listener registered
2020-03-19 16:38:11,063 2446 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 82 
2020-03-19 16:38:11,068 2451 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 50 
2020-03-19 16:38:11,068 2451 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 50
2020-03-19 16:38:11,068 2451 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 50
2020-03-19 16:38:11,068 2451 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 50 service start completed
2020-03-19 16:38:11,068 2451 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 51 change notification listener registered
2020-03-19 16:38:11,075 2458 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 55
2020-03-19 16:38:11,075 2458 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 55 
2020-03-19 16:38:11,075 2458 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 55
2020-03-19 16:38:11,075 2458 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 55 service start completed
2020-03-19 16:38:11,075 2458 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 90 change notification listener registered
2020-03-19 16:38:11,078 2461 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 76 
2020-03-19 16:38:11,078 2461 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 76
2020-03-19 16:38:11,079 2462 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 90
2020-03-19 16:38:11,080 2463 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 90
2020-03-19 16:38:11,080 2463 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 90 
2020-03-19 16:38:11,080 2463 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 90 service start completed
2020-03-19 16:38:11,080 2463 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 88 change notification listener registered
2020-03-19 16:38:11,080 2463 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 76
2020-03-19 16:38:11,080 2463 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 76 service start completed
2020-03-19 16:38:11,081 2464 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 49 change notification listener registered
2020-03-19 16:38:11,088 2471 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 51
2020-03-19 16:38:11,088 2471 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 51 
2020-03-19 16:38:11,088 2471 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 51
2020-03-19 16:38:11,088 2471 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 51 service start completed
2020-03-19 16:38:11,089 2472 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 43 change notification listener registered
2020-03-19 16:38:11,094 2477 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 49
2020-03-19 16:38:11,094 2477 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 49 
2020-03-19 16:38:11,094 2477 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 49
2020-03-19 16:38:11,094 2477 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 49 service start completed
2020-03-19 16:38:11,094 2477 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 48 change notification listener registered
2020-03-19 16:38:11,102 2485 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 88
2020-03-19 16:38:11,102 2485 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 88 
2020-03-19 16:38:11,102 2485 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 88
2020-03-19 16:38:11,102 2485 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 88 service start completed
2020-03-19 16:38:11,102 2485 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 47 change notification listener registered
2020-03-19 16:38:11,110 2493 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 43
2020-03-19 16:38:11,110 2493 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 43
2020-03-19 16:38:11,110 2493 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 48
2020-03-19 16:38:11,110 2493 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 43 service start completed
2020-03-19 16:38:11,111 2494 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 43 
2020-03-19 16:38:11,111 2494 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 48 
2020-03-19 16:38:11,111 2494 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 48
2020-03-19 16:38:11,111 2494 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 27 change notification listener registered
2020-03-19 16:38:11,112 2495 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 48 service start completed
2020-03-19 16:38:11,112 2495 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 46 change notification listener registered
2020-03-19 16:38:11,112 2495 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 47
2020-03-19 16:38:11,113 2496 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 47
2020-03-19 16:38:11,113 2496 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 47 
2020-03-19 16:38:11,113 2496 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 47 service start completed
2020-03-19 16:38:11,113 2496 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 41 change notification listener registered
2020-03-19 16:38:11,113 2496 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 46
2020-03-19 16:38:11,113 2496 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 46
2020-03-19 16:38:11,114 2497 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 46 
2020-03-19 16:38:11,114 2497 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 46 service start completed
2020-03-19 16:38:11,114 2497 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 92 change notification listener registered
2020-03-19 16:38:11,114 2497 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 27
2020-03-19 16:38:11,114 2497 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 27
2020-03-19 16:38:11,114 2497 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 27 service start completed
2020-03-19 16:38:11,114 2497 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 27 
2020-03-19 16:38:11,115 2498 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 42 change notification listener registered
2020-03-19 16:38:11,115 2498 [watermarkingpool-5] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 41 
2020-03-19 16:38:11,116 2499 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 41
2020-03-19 16:38:11,116 2499 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 41
2020-03-19 16:38:11,116 2499 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 41 service start completed
2020-03-19 16:38:11,116 2499 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 93 change notification listener registered
2020-03-19 16:38:11,122 2505 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 42
2020-03-19 16:38:11,123 2506 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 42
2020-03-19 16:38:11,123 2506 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 42 service start completed
2020-03-19 16:38:11,123 2506 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 17 change notification listener registered
2020-03-19 16:38:11,123 2506 [watermarkingpool-4] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 42 
2020-03-19 16:38:11,134 2517 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 92 
2020-03-19 16:38:11,134 2517 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 92
2020-03-19 16:38:11,134 2517 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 92
2020-03-19 16:38:11,134 2517 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 92 service start completed
2020-03-19 16:38:11,134 2517 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 35 change notification listener registered
2020-03-19 16:38:11,137 2520 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 93
2020-03-19 16:38:11,137 2520 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 17
2020-03-19 16:38:11,138 2521 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 93
2020-03-19 16:38:11,138 2521 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 93 
2020-03-19 16:38:11,138 2521 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 17
2020-03-19 16:38:11,138 2521 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 17 service start completed
2020-03-19 16:38:11,138 2521 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 93 service start completed
2020-03-19 16:38:11,138 2521 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 17 
2020-03-19 16:38:11,138 2521 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 81 change notification listener registered
2020-03-19 16:38:11,138 2521 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 54 change notification listener registered
2020-03-19 16:38:11,143 2526 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 35
2020-03-19 16:38:11,144 2527 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 35
2020-03-19 16:38:11,144 2527 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 35 service start completed
2020-03-19 16:38:11,144 2527 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 33 change notification listener registered
2020-03-19 16:38:11,145 2528 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 35 
2020-03-19 16:38:11,155 2538 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0] REGISTERED
2020-03-19 16:38:11,156 2539 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 81 
2020-03-19 16:38:11,156 2539 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 81
2020-03-19 16:38:11,156 2539 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 81
2020-03-19 16:38:11,156 2539 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 81 service start completed
2020-03-19 16:38:11,157 2540 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 25 change notification listener registered
2020-03-19 16:38:11,158 2541 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0] BIND: /127.0.1.1:6000
2020-03-19 16:38:11,160 2543 [main] INFO  i.p.s.server.host.ServiceStarter - PravegaConnectionListener started successfully.
2020-03-19 16:38:11,161 2544 [main] INFO  i.p.s.server.host.ServiceStarter - StreamSegmentService started.
Pravega Sandbox is running locally now. You could access it at 127.0.0.1:9090
2020-03-19 16:38:11,163 2546 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] ACTIVE
2020-03-19 16:38:11,166 2549 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 54 
2020-03-19 16:38:11,166 2549 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 54
2020-03-19 16:38:11,166 2549 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 54
2020-03-19 16:38:11,166 2549 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 54 service start completed
2020-03-19 16:38:11,166 2549 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 56 change notification listener registered
2020-03-19 16:38:11,174 2557 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 33
2020-03-19 16:38:11,174 2557 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 33
2020-03-19 16:38:11,174 2557 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 33 service start completed
2020-03-19 16:38:11,174 2557 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 24 change notification listener registered
2020-03-19 16:38:11,175 2558 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 33 
2020-03-19 16:38:11,180 2563 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 25 
2020-03-19 16:38:11,180 2563 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 25
2020-03-19 16:38:11,181 2564 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 25
2020-03-19 16:38:11,181 2564 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 25 service start completed
2020-03-19 16:38:11,181 2564 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 56
2020-03-19 16:38:11,181 2564 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 56 
2020-03-19 16:38:11,181 2564 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 34 change notification listener registered
2020-03-19 16:38:11,182 2565 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 56
2020-03-19 16:38:11,182 2565 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 56 service start completed
2020-03-19 16:38:11,183 2566 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 16 change notification listener registered
2020-03-19 16:38:11,186 2569 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 24
2020-03-19 16:38:11,186 2569 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 24 
2020-03-19 16:38:11,188 2571 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 24
2020-03-19 16:38:11,188 2571 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 24 service start completed
2020-03-19 16:38:11,189 2572 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 28 change notification listener registered
2020-03-19 16:38:11,196 2579 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 16 
2020-03-19 16:38:11,196 2579 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 16
2020-03-19 16:38:11,196 2579 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 16
2020-03-19 16:38:11,197 2580 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 16 service start completed
2020-03-19 16:38:11,197 2580 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 15 change notification listener registered
2020-03-19 16:38:11,198 2581 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 34
2020-03-19 16:38:11,198 2581 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 34
2020-03-19 16:38:11,198 2581 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 34 service start completed
2020-03-19 16:38:11,198 2581 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 45 change notification listener registered
2020-03-19 16:38:11,199 2582 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 34 
2020-03-19 16:38:11,202 2585 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 28
2020-03-19 16:38:11,202 2585 [watermarkingpool-4] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 28 
2020-03-19 16:38:11,202 2585 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 28
2020-03-19 16:38:11,202 2585 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 28 service start completed
2020-03-19 16:38:11,202 2585 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 6 change notification listener registered
2020-03-19 16:38:11,206 2589 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 15 
2020-03-19 16:38:11,209 2592 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 15
2020-03-19 16:38:11,209 2592 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 15
2020-03-19 16:38:11,210 2593 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 15 service start completed
2020-03-19 16:38:11,210 2593 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 39 change notification listener registered
2020-03-19 16:38:11,210 2593 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 45
2020-03-19 16:38:11,210 2593 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 45 
2020-03-19 16:38:11,211 2594 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 45
2020-03-19 16:38:11,211 2594 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 45 service start completed
2020-03-19 16:38:11,211 2594 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 26 change notification listener registered
2020-03-19 16:38:11,211 2594 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 6
2020-03-19 16:38:11,212 2595 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 6
2020-03-19 16:38:11,212 2595 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 6 service start completed
2020-03-19 16:38:11,212 2595 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 39
2020-03-19 16:38:11,212 2595 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 39
2020-03-19 16:38:11,212 2595 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 39 service start completed
2020-03-19 16:38:11,212 2595 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 40 change notification listener registered
2020-03-19 16:38:11,212 2595 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 96 change notification listener registered
2020-03-19 16:38:11,212 2595 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 6 
2020-03-19 16:38:11,212 2595 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 39 
2020-03-19 16:38:11,214 2597 [watermarkingpool-6] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 26 
2020-03-19 16:38:11,214 2597 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 26
2020-03-19 16:38:11,214 2597 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 26
2020-03-19 16:38:11,214 2597 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 26 service start completed
2020-03-19 16:38:11,215 2598 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 40
2020-03-19 16:38:11,215 2598 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 5 change notification listener registered
2020-03-19 16:38:11,215 2598 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 40
2020-03-19 16:38:11,215 2598 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 40 service start completed
2020-03-19 16:38:11,215 2598 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 44 change notification listener registered
2020-03-19 16:38:11,218 2601 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 40 
2020-03-19 16:38:11,222 2605 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 5
2020-03-19 16:38:11,222 2605 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 5 
2020-03-19 16:38:11,222 2605 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 5
2020-03-19 16:38:11,222 2605 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 5 service start completed
2020-03-19 16:38:11,222 2605 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 96
2020-03-19 16:38:11,222 2605 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 96
2020-03-19 16:38:11,222 2605 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 96 
2020-03-19 16:38:11,222 2605 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 96 service start completed
2020-03-19 16:38:11,222 2605 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 95 change notification listener registered
2020-03-19 16:38:11,223 2606 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 32 change notification listener registered
2020-03-19 16:38:11,232 2615 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 95 
2020-03-19 16:38:11,232 2615 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 95
2020-03-19 16:38:11,233 2616 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 95
2020-03-19 16:38:11,233 2616 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 95 service start completed
2020-03-19 16:38:11,233 2616 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 31 change notification listener registered
2020-03-19 16:38:11,234 2617 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 32
2020-03-19 16:38:11,234 2617 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 32
2020-03-19 16:38:11,234 2617 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 32 service start completed
2020-03-19 16:38:11,234 2617 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 91 change notification listener registered
2020-03-19 16:38:11,234 2617 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 32 
2020-03-19 16:38:11,240 2623 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 31
2020-03-19 16:38:11,240 2623 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 31 
2020-03-19 16:38:11,241 2624 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 31
2020-03-19 16:38:11,241 2624 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 31 service start completed
2020-03-19 16:38:11,241 2624 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 38 change notification listener registered
2020-03-19 16:38:11,246 2629 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 38
2020-03-19 16:38:11,246 2629 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 38
2020-03-19 16:38:11,246 2629 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 38 service start completed
2020-03-19 16:38:11,246 2629 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 37 change notification listener registered
2020-03-19 16:38:11,250 2633 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 38 
2020-03-19 16:38:11,250 2633 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 37 
2020-03-19 16:38:11,250 2633 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 37
2020-03-19 16:38:11,250 2633 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 37
2020-03-19 16:38:11,251 2634 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 37 service start completed
2020-03-19 16:38:11,251 2634 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 98 change notification listener registered
2020-03-19 16:38:11,256 2639 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 44 
2020-03-19 16:38:11,256 2639 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 44
2020-03-19 16:38:11,257 2640 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 44
2020-03-19 16:38:11,260 2643 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 44 service start completed
2020-03-19 16:38:11,260 2643 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 23 change notification listener registered
2020-03-19 16:38:11,262 2645 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 98
2020-03-19 16:38:11,262 2645 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 98 
2020-03-19 16:38:11,263 2646 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 98
2020-03-19 16:38:11,263 2646 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 91
2020-03-19 16:38:11,263 2646 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 98 service start completed
2020-03-19 16:38:11,263 2646 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 91 
2020-03-19 16:38:11,263 2646 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 91
2020-03-19 16:38:11,263 2646 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 91 service start completed
2020-03-19 16:38:11,263 2646 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 22 change notification listener registered
2020-03-19 16:38:11,263 2646 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 30 change notification listener registered
2020-03-19 16:38:11,264 2647 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 22
2020-03-19 16:38:11,264 2647 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 22 
2020-03-19 16:38:11,265 2648 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 22
2020-03-19 16:38:11,265 2648 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 22 service start completed
2020-03-19 16:38:11,265 2648 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 14 change notification listener registered
2020-03-19 16:38:11,266 2649 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 30
2020-03-19 16:38:11,266 2649 [watermarkingpool-4] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 30 
2020-03-19 16:38:11,266 2649 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 30
2020-03-19 16:38:11,267 2650 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 30 service start completed
2020-03-19 16:38:11,268 2651 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 29 change notification listener registered
2020-03-19 16:38:11,270 2653 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 29
2020-03-19 16:38:11,270 2653 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 29
2020-03-19 16:38:11,270 2653 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 29 service start completed
2020-03-19 16:38:11,270 2653 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 19 change notification listener registered
2020-03-19 16:38:11,271 2654 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 19
2020-03-19 16:38:11,271 2654 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 19
2020-03-19 16:38:11,271 2654 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 19 service start completed
2020-03-19 16:38:11,272 2655 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 20 change notification listener registered
2020-03-19 16:38:11,273 2656 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 20
2020-03-19 16:38:11,273 2656 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 20
2020-03-19 16:38:11,273 2656 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 20 service start completed
2020-03-19 16:38:11,273 2656 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 21 change notification listener registered
2020-03-19 16:38:11,274 2657 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 29 
2020-03-19 16:38:11,274 2657 [watermarkingpool-4] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 19 
2020-03-19 16:38:11,274 2657 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 20 
2020-03-19 16:38:11,282 2665 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 14
2020-03-19 16:38:11,282 2665 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 14
2020-03-19 16:38:11,282 2665 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 14 service start completed
2020-03-19 16:38:11,282 2665 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 14 
2020-03-19 16:38:11,282 2665 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 13 change notification listener registered
2020-03-19 16:38:11,287 2670 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 23
2020-03-19 16:38:11,287 2670 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 23 
2020-03-19 16:38:11,287 2670 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 23
2020-03-19 16:38:11,287 2670 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 23 service start completed
2020-03-19 16:38:11,287 2670 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 8 change notification listener registered
2020-03-19 16:38:11,292 2675 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 21
2020-03-19 16:38:11,293 2676 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 21
2020-03-19 16:38:11,293 2676 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 21 service start completed
2020-03-19 16:38:11,293 2676 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 99 change notification listener registered
2020-03-19 16:38:11,294 2677 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 21 
2020-03-19 16:38:11,299 2682 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 13
2020-03-19 16:38:11,300 2683 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 13 
2020-03-19 16:38:11,300 2683 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 13
2020-03-19 16:38:11,300 2683 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 13 service start completed
2020-03-19 16:38:11,300 2683 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 94 change notification listener registered
2020-03-19 16:38:11,300 2683 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 8
2020-03-19 16:38:11,300 2683 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 8 
2020-03-19 16:38:11,302 2685 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 8
2020-03-19 16:38:11,302 2685 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 8 service start completed
2020-03-19 16:38:11,302 2685 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 18 change notification listener registered
2020-03-19 16:38:11,310 2693 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 99
2020-03-19 16:38:11,311 2694 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 99
2020-03-19 16:38:11,311 2694 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 99 service start completed
2020-03-19 16:38:11,311 2694 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 9 change notification listener registered
2020-03-19 16:38:11,311 2694 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 99 
2020-03-19 16:38:11,318 2701 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 94 
2020-03-19 16:38:11,319 2702 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 18
2020-03-19 16:38:11,319 2702 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 18 
2020-03-19 16:38:11,319 2702 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 18
2020-03-19 16:38:11,319 2702 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 18 service start completed
2020-03-19 16:38:11,319 2702 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 94
2020-03-19 16:38:11,319 2702 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 94
2020-03-19 16:38:11,319 2702 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 94 service start completed
2020-03-19 16:38:11,319 2702 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 89 change notification listener registered
2020-03-19 16:38:11,320 2703 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 7 change notification listener registered
2020-03-19 16:38:11,325 2708 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 9
2020-03-19 16:38:11,325 2708 [watermarkingpool-9] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 9 
2020-03-19 16:38:11,325 2708 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 9
2020-03-19 16:38:11,325 2708 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 9 service start completed
2020-03-19 16:38:11,325 2708 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 87 change notification listener registered
2020-03-19 16:38:11,331 2714 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 89 
2020-03-19 16:38:11,331 2714 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 89
2020-03-19 16:38:11,332 2715 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 89
2020-03-19 16:38:11,332 2715 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 89 service start completed
2020-03-19 16:38:11,332 2715 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 12 change notification listener registered
2020-03-19 16:38:11,337 2720 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 7
2020-03-19 16:38:11,338 2721 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 7 
2020-03-19 16:38:11,338 2721 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 7
2020-03-19 16:38:11,338 2721 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 7 service start completed
2020-03-19 16:38:11,338 2721 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 11 change notification listener registered
2020-03-19 16:38:11,338 2721 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 12
2020-03-19 16:38:11,338 2721 [watermarkingpool-2] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 12 
2020-03-19 16:38:11,338 2721 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 12
2020-03-19 16:38:11,338 2721 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 12 service start completed
2020-03-19 16:38:11,339 2722 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 79 change notification listener registered
2020-03-19 16:38:11,350 2733 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 87
2020-03-19 16:38:11,350 2733 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 87 
2020-03-19 16:38:11,350 2733 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 87
2020-03-19 16:38:11,350 2733 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 87 service start completed
2020-03-19 16:38:11,350 2733 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 79
2020-03-19 16:38:11,351 2734 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 10 change notification listener registered
2020-03-19 16:38:11,351 2734 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 79
2020-03-19 16:38:11,351 2734 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 79 service start completed
2020-03-19 16:38:11,351 2734 [watermarkingpool-8] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 79 
2020-03-19 16:38:11,351 2734 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 97 change notification listener registered
2020-03-19 16:38:11,352 2735 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 11
2020-03-19 16:38:11,353 2736 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 11
2020-03-19 16:38:11,353 2736 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 11 service start completed
2020-03-19 16:38:11,353 2736 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 36 change notification listener registered
2020-03-19 16:38:11,353 2736 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 11 
2020-03-19 16:38:11,357 2740 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 10
2020-03-19 16:38:11,358 2741 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 10
2020-03-19 16:38:11,358 2741 [watermarkingpool-1] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 10 
2020-03-19 16:38:11,358 2741 [watermarkingpool-7] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 36 
2020-03-19 16:38:11,358 2741 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 10 service start completed
2020-03-19 16:38:11,358 2741 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 36
2020-03-19 16:38:11,358 2741 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 36
2020-03-19 16:38:11,359 2742 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 84 change notification listener registered
2020-03-19 16:38:11,359 2742 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 36 service start completed
2020-03-19 16:38:11,359 2742 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.s.b.ZooKeeperBucketService - bucket 75 change notification listener registered
2020-03-19 16:38:11,360 2743 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 97
2020-03-19 16:38:11,360 2743 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 97
2020-03-19 16:38:11,360 2743 [watermarkingpool-3] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 97 
2020-03-19 16:38:11,360 2743 [ForkJoinPool.commonPool-worker-1] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 97 service start completed
2020-03-19 16:38:11,367 2750 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 84
2020-03-19 16:38:11,368 2751 [watermarkingpool-5] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 84 
2020-03-19 16:38:11,368 2751 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 84
2020-03-19 16:38:11,368 2751 [ForkJoinPool.commonPool-worker-3] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 84 service start completed
2020-03-19 16:38:11,370 2753 [watermarkingpool-10] INFO  i.p.c.server.bucket.BucketManager - WatermarkingService: successfully started bucket service bucket: 75 
2020-03-19 16:38:11,370 2753 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 75
2020-03-19 16:38:11,370 2753 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: Notification loop started for bucket 75
2020-03-19 16:38:11,370 2753 [ForkJoinPool.commonPool-worker-2] INFO  i.p.c.server.bucket.BucketService - WatermarkingService: bucket 75 service start completed
2020-03-19 16:38:11,370 2753 [watermarkingpool-10] INFO  i.p.c.s.b.ZooKeeperBucketManager - bucket ownership listener registered on bucket root WatermarkingService
2020-03-19 16:38:11,459 2842 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Starting event processors
2020-03-19 16:38:11,460 2843 [ControllerServiceStarter STARTING] INFO  i.p.c.s.e.ControllerEventProcessors - Bootstrapping controller event processors
2020-03-19 16:38:11,543 2926 [controllerpool-28] INFO  i.p.c.s.e.ControllerEventProcessors - Created controller scope _system
2020-03-19 16:38:11,748 3131 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Awaiting start of rpc server
2020-03-19 16:38:11,754 3137 [GRPCServer STARTING] INFO  i.p.c.server.rpc.grpc.GRPCServer - Starting gRPC server listening on port: 9090
2020-03-19 16:38:11,942 3325 [controllerpool-66] INFO  i.p.c.t.Stream.StreamMetadataTasks - _system/_abortStream created in metadata store
2020-03-19 16:38:11,953 3336 [controllerpool-10] INFO  i.p.c.t.Stream.StreamMetadataTasks - _system/_requeststream created in metadata store
2020-03-19 16:38:11,969 3352 [controllerpool-14] INFO  i.p.c.t.Stream.StreamMetadataTasks - _system/_commitStream created in metadata store
2020-03-19 16:38:12,298 3681 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Awaiting start of REST server
2020-03-19 16:38:12,299 3682 [RESTServer STARTING] INFO  i.p.c.server.rest.RESTServer - Starting REST server listening on port: 9091
2020-03-19 16:38:12,792 4175 [grpc-default-executor-0] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - [requestId=5507811861068806083] createScope called for scope testScope.
2020-03-19 16:38:12,878 4261 [grpc-default-executor-0] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - [requestId=-2590570089484799806] createStream called for stream testScope/testStream.
2020-03-19 16:38:13,040 4423 [controllerpool-36] INFO  i.p.c.t.Stream.StreamMetadataTasks - [requestId=-2590570089484799806] testScope/testStream created in metadata store
2020-03-19 16:38:14,239 5622 [ControllerServiceStarter STARTING] INFO  i.p.c.s.ControllerServiceStarter - Awaiting start of controller event processors
2020-03-19 16:38:20,135 11518 [Curator-LeaderSelector-0] INFO  i.p.c.f.UniformContainerBalancer - Creating new balanced map
2020-03-19 16:38:20,154 11537 [Curator-LeaderSelector-0] INFO  i.p.c.store.host.ZKHostStore - Successfully updated segment container map
2020-03-19 16:38:20,158 11541 [core-8] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Container Changes: Desired = [0, 1, 2, 3], Current = [], PendingTasks = [], ToStart = [0, 1, 2, 3], ToStop = [].
2020-03-19 16:38:20,159 11542 [core-8] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Starting Container 0.
2020-03-19 16:38:20,190 11573 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_0]: Created empty database directory '/tmp/pravega/cache/cache_Container_0'.
2020-03-19 16:38:20,207 11590 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_0]: Initialized.
2020-03-19 16:38:20,245 11628 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_0_Attributes]: Created empty database directory '/tmp/pravega/cache/cache_Container_0_Attributes'.
2020-03-19 16:38:20,275 11658 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_0_Attributes]: Initialized.
2020-03-19 16:38:20,292 11675 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_0_TableKeys]: Created empty database directory '/tmp/pravega/cache/cache_Container_0_TableKeys'.
2020-03-19 16:38:20,325 11708 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_0_TableKeys]: Initialized.
2020-03-19 16:38:20,326 11709 [core-8] INFO  i.p.s.server.CacheManager - CacheManager Registered io.pravega.segmentstore.server.tables.ContainerKeyCache@2bdc02c7.
2020-03-19 16:38:20,337 11720 [core-8] INFO  i.p.s.s.s.StreamSegmentContainerRegistry - Registered SegmentContainer 0.
2020-03-19 16:38:20,338 11721 [core-8] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[0]: Starting.
2020-03-19 16:38:20,339 11722 [core-8] INFO  i.p.s.server.logs.DurableLog - DurableLog[0]: Starting.
2020-03-19 16:38:20,344 11727 [core-8] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Starting Container 1.
2020-03-19 16:38:20,345 11728 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_1]: Created empty database directory '/tmp/pravega/cache/cache_Container_1'.
2020-03-19 16:38:20,350 11733 [core-9] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[0] Recovery started.
2020-03-19 16:38:20,350 11733 [core-9] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[0]: Enter RecoveryMode.
2020-03-19 16:38:20,350 11733 [core-9] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[0]: Reset.
2020-03-19 16:38:20,350 11733 [core-9] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[0] Enter RecoveryMode.
2020-03-19 16:38:20,351 11734 [core-9] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[0]: Cleared.
2020-03-19 16:38:20,356 11739 [core-9] WARN  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[0]: Reached the end of the DataFrameLog and could not find any MetadataCheckpointOperations after reading 0 Operations and 0 Data Frames.
2020-03-19 16:38:20,356 11739 [core-9] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[0] Recovery completed. Epoch = 1, Items Recovered = 0, Time = 6ms.
2020-03-19 16:38:20,356 11739 [core-9] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[0]: Exit RecoveryMode.
2020-03-19 16:38:20,357 11740 [core-9] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[0] Exit RecoveryMode.
2020-03-19 16:38:20,358 11741 [core-9] INFO  i.p.c.c.AbstractThreadPoolService - OperationProcessor[0]: Started.
2020-03-19 16:38:20,382 11765 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_1]: Initialized.
2020-03-19 16:38:20,383 11766 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_1_Attributes]: Created empty database directory '/tmp/pravega/cache/cache_Container_1_Attributes'.
2020-03-19 16:38:20,404 11787 [core-5] INFO  i.p.s.server.logs.DurableLog - DurableLog[0]: MetadataCheckpointOperation durably stored.
2020-03-19 16:38:20,409 11792 [core-5] INFO  i.p.s.server.logs.DurableLog - DurableLog[0]: Started (Online).
2020-03-19 16:38:20,430 11813 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_1_Attributes]: Initialized.
2020-03-19 16:38:20,431 11814 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_1_TableKeys]: Created empty database directory '/tmp/pravega/cache/cache_Container_1_TableKeys'.
2020-03-19 16:38:20,450 11833 [core-18] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[0]: MapStreamSegment SegmentId = 1, Name = '_system/containers/metadata_0', Active = 1
2020-03-19 16:38:20,456 11839 [core-18] INFO  i.p.s.s.c.TableMetadataStore - MetadataStore[0]: Metadata Segment pinned. Name = '_system/containers/metadata_0', Id = '1'
2020-03-19 16:38:20,457 11840 [core-11] INFO  i.p.c.c.AbstractThreadPoolService - SegmentContainer[0]: Started.
2020-03-19 16:38:20,470 11853 [core-14] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Container 0 has been registered.
2020-03-19 16:38:20,472 11855 [core-11] INFO  i.p.c.c.AbstractThreadPoolService - StorageWriter[0]: Started.
2020-03-19 16:38:20,475 11858 [core-11] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[0]: Started.
2020-03-19 16:38:20,484 11867 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_1_TableKeys]: Initialized.
2020-03-19 16:38:20,484 11867 [core-8] INFO  i.p.s.server.CacheManager - CacheManager Registered io.pravega.segmentstore.server.tables.ContainerKeyCache@4899237.
2020-03-19 16:38:20,485 11868 [core-8] INFO  i.p.s.s.s.StreamSegmentContainerRegistry - Registered SegmentContainer 1.
2020-03-19 16:38:20,485 11868 [core-8] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[1]: Starting.
2020-03-19 16:38:20,485 11868 [core-8] INFO  i.p.s.server.logs.DurableLog - DurableLog[1]: Starting.
2020-03-19 16:38:20,485 11868 [core-8] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Starting Container 2.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[1] Recovery started.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[1]: Enter RecoveryMode.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[1]: Reset.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[1] Enter RecoveryMode.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[1]: Cleared.
2020-03-19 16:38:20,488 11871 [core-6] WARN  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[1]: Reached the end of the DataFrameLog and could not find any MetadataCheckpointOperations after reading 0 Operations and 0 Data Frames.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[1] Recovery completed. Epoch = 1, Items Recovered = 0, Time = 0ms.
2020-03-19 16:38:20,488 11871 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_2]: Created empty database directory '/tmp/pravega/cache/cache_Container_2'.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[1]: Exit RecoveryMode.
2020-03-19 16:38:20,488 11871 [core-6] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[1] Exit RecoveryMode.
2020-03-19 16:38:20,490 11873 [core-6] INFO  i.p.c.c.AbstractThreadPoolService - OperationProcessor[1]: Started.
2020-03-19 16:38:20,498 11881 [core-1] INFO  i.p.s.server.logs.DurableLog - DurableLog[1]: MetadataCheckpointOperation durably stored.
2020-03-19 16:38:20,498 11881 [core-1] INFO  i.p.s.server.logs.DurableLog - DurableLog[1]: Started (Online).
2020-03-19 16:38:20,501 11884 [core-1] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[1]: MapStreamSegment SegmentId = 1, Name = '_system/containers/metadata_1', Active = 1
2020-03-19 16:38:20,501 11884 [core-1] INFO  i.p.s.s.c.TableMetadataStore - MetadataStore[1]: Metadata Segment pinned. Name = '_system/containers/metadata_1', Id = '1'
2020-03-19 16:38:20,502 11885 [core-12] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Container 1 has been registered.
2020-03-19 16:38:20,503 11886 [core-1] INFO  i.p.c.c.AbstractThreadPoolService - SegmentContainer[1]: Started.
2020-03-19 16:38:20,507 11890 [core-1] INFO  i.p.c.c.AbstractThreadPoolService - StorageWriter[1]: Started.
2020-03-19 16:38:20,510 11893 [core-1] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[1]: Started.
2020-03-19 16:38:20,523 11906 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_2]: Initialized.
2020-03-19 16:38:20,524 11907 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_2_Attributes]: Created empty database directory '/tmp/pravega/cache/cache_Container_2_Attributes'.
2020-03-19 16:38:20,567 11950 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_2_Attributes]: Initialized.
2020-03-19 16:38:20,568 11951 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_2_TableKeys]: Created empty database directory '/tmp/pravega/cache/cache_Container_2_TableKeys'.
2020-03-19 16:38:20,614 11997 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_2_TableKeys]: Initialized.
2020-03-19 16:38:20,614 11997 [core-8] INFO  i.p.s.server.CacheManager - CacheManager Registered io.pravega.segmentstore.server.tables.ContainerKeyCache@1fb2e339.
2020-03-19 16:38:20,614 11997 [core-8] INFO  i.p.s.s.s.StreamSegmentContainerRegistry - Registered SegmentContainer 2.
2020-03-19 16:38:20,615 11998 [core-8] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[2]: Starting.
2020-03-19 16:38:20,615 11998 [core-8] INFO  i.p.s.server.logs.DurableLog - DurableLog[2]: Starting.
2020-03-19 16:38:20,615 11998 [core-8] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Starting Container 3.
2020-03-19 16:38:20,615 11998 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_3]: Created empty database directory '/tmp/pravega/cache/cache_Container_3'.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[2] Recovery started.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: Enter RecoveryMode.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: Reset.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[2] Enter RecoveryMode.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[2]: Cleared.
2020-03-19 16:38:20,617 12000 [core-2] WARN  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[2]: Reached the end of the DataFrameLog and could not find any MetadataCheckpointOperations after reading 0 Operations and 0 Data Frames.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[2] Recovery completed. Epoch = 1, Items Recovered = 0, Time = 0ms.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: Exit RecoveryMode.
2020-03-19 16:38:20,617 12000 [core-2] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[2] Exit RecoveryMode.
2020-03-19 16:38:20,619 12002 [core-2] INFO  i.p.c.c.AbstractThreadPoolService - OperationProcessor[2]: Started.
2020-03-19 16:38:20,620 12003 [core-2] INFO  i.p.s.server.logs.DurableLog - DurableLog[2]: MetadataCheckpointOperation durably stored.
2020-03-19 16:38:20,620 12003 [core-2] INFO  i.p.s.server.logs.DurableLog - DurableLog[2]: Started (Online).
2020-03-19 16:38:20,637 12020 [core-18] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: MapStreamSegment SegmentId = 1, Name = '_system/containers/metadata_2', Active = 1
2020-03-19 16:38:20,637 12020 [core-18] INFO  i.p.s.s.c.TableMetadataStore - MetadataStore[2]: Metadata Segment pinned. Name = '_system/containers/metadata_2', Id = '1'
2020-03-19 16:38:20,638 12021 [core-4] INFO  i.p.c.c.AbstractThreadPoolService - SegmentContainer[2]: Started.
2020-03-19 16:38:20,638 12021 [core-3] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Container 2 has been registered.
2020-03-19 16:38:20,641 12024 [core-4] INFO  i.p.c.c.AbstractThreadPoolService - StorageWriter[2]: Started.
2020-03-19 16:38:20,643 12026 [core-15] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[2]: Started.
2020-03-19 16:38:20,663 12046 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_3]: Initialized.
2020-03-19 16:38:20,664 12047 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_3_Attributes]: Created empty database directory '/tmp/pravega/cache/cache_Container_3_Attributes'.
2020-03-19 16:38:20,707 12090 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_3_Attributes]: Initialized.
2020-03-19 16:38:20,708 12091 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_3_TableKeys]: Created empty database directory '/tmp/pravega/cache/cache_Container_3_TableKeys'.
2020-03-19 16:38:20,746 12129 [auto-scaler-4] INFO  i.p.c.stream.impl.ClientFactoryImpl - Creating writer: 18910f78-3c23-4750-9a67-883d80b83959 for stream: _requeststream with configuration: EventWriterConfig(initalBackoffMillis=1, maxBackoffMillis=20000, retryAttempts=10, backoffMultiple=10, enableConnectionPooling=false, transactionTimeoutTime=89999, automaticallyNoteTime=false)
2020-03-19 16:38:20,754 12137 [auto-scaler-4] INFO  i.p.c.stream.impl.SegmentSelector - Refreshing segments for stream StreamImpl(scope=_system, streamName=_requeststream)
2020-03-19 16:38:20,754 12137 [core-8] INFO  i.p.s.s.impl.rocksdb.RocksDBCache - RocksDBCache[Container_3_TableKeys]: Initialized.
2020-03-19 16:38:20,754 12137 [core-8] INFO  i.p.s.server.CacheManager - CacheManager Registered io.pravega.segmentstore.server.tables.ContainerKeyCache@2e3144e0.
2020-03-19 16:38:20,754 12137 [core-8] INFO  i.p.s.s.s.StreamSegmentContainerRegistry - Registered SegmentContainer 3.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[3]: Starting.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.server.logs.DurableLog - DurableLog[3]: Starting.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[3] Recovery started.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[3]: Enter RecoveryMode.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[3]: Reset.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[3] Enter RecoveryMode.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[3]: Cleared.
2020-03-19 16:38:20,755 12138 [core-8] WARN  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[3]: Reached the end of the DataFrameLog and could not find any MetadataCheckpointOperations after reading 0 Operations and 0 Data Frames.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.server.logs.RecoveryProcessor - RecoveryProcessor[3] Recovery completed. Epoch = 1, Items Recovered = 0, Time = 0ms.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[3]: Exit RecoveryMode.
2020-03-19 16:38:20,755 12138 [core-8] INFO  i.p.s.s.reading.ContainerReadIndex - ReadIndex[3] Exit RecoveryMode.
2020-03-19 16:38:20,756 12139 [core-8] INFO  i.p.c.c.AbstractThreadPoolService - OperationProcessor[3]: Started.
2020-03-19 16:38:20,766 12149 [core-7] INFO  i.p.s.server.logs.DurableLog - DurableLog[3]: MetadataCheckpointOperation durably stored.
2020-03-19 16:38:20,767 12150 [core-7] INFO  i.p.s.server.logs.DurableLog - DurableLog[3]: Started (Online).
2020-03-19 16:38:20,770 12153 [core-7] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[3]: MapStreamSegment SegmentId = 1, Name = '_system/containers/metadata_3', Active = 1
2020-03-19 16:38:20,771 12154 [core-7] INFO  i.p.s.s.c.TableMetadataStore - MetadataStore[3]: Metadata Segment pinned. Name = '_system/containers/metadata_3', Id = '1'
2020-03-19 16:38:20,775 12158 [core-11] INFO  i.p.c.c.AbstractThreadPoolService - SegmentContainer[3]: Started.
2020-03-19 16:38:20,778 12161 [core-7] INFO  i.p.s.s.h.ZKSegmentContainerMonitor - Container 3 has been registered.
2020-03-19 16:38:20,778 12161 [core-11] INFO  i.p.c.c.AbstractThreadPoolService - StorageWriter[3]: Started.
2020-03-19 16:38:20,782 12165 [core-11] INFO  i.p.s.s.c.StreamSegmentContainer - SegmentContainer[3]: Started.
2020-03-19 16:38:20,806 12189 [fetch-controllers-1] INFO  i.p.c.s.i.ControllerResolverFactory - Attempting to refresh the controller server endpoints
2020-03-19 16:38:20,807 12190 [fetch-controllers-1] INFO  i.p.c.s.i.ControllerResolverFactory - Updating client with controllers: [[[localhost/127.0.0.1:9090]/{}]]
2020-03-19 16:38:20,841 12224 [fetch-controllers-1] INFO  i.p.c.s.i.ControllerResolverFactory - Rescheduling ControllerNameResolver task for after 120000 ms
2020-03-19 16:38:20,934 12317 [grpc-default-executor-0] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getCurrentSegments called for stream _system/_requeststream.
2020-03-19 16:38:20,948 12331 [controllerpool-18] ERROR i.p.c.s.r.g.v.ControllerServiceImpl - Controller API call with tag none failed with error: 
io.pravega.controller.store.stream.StoreException$IllegalStateException: Stream: _requeststream State: CREATING
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:99)
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:70)
	at io.pravega.controller.store.stream.PersistentStreamBase.lambda$verifyLegalState$202(PersistentStreamBase.java:1700)
	at java.util.concurrent.CompletableFuture.uniApply(CompletableFuture.java:602)
	at java.util.concurrent.CompletableFuture$UniApply.tryFire(CompletableFuture.java:577)
	at java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:474)
	at java.util.concurrent.CompletableFuture.complete(CompletableFuture.java:1962)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$getData$12(ZKStoreHelper.java:182)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$callback$30(ZKStoreHelper.java:402)
	at org.apache.curator.framework.imps.Backgrounding$1$1.run(Backgrounding.java:158)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:20,960 12343 [grpc-default-executor-0] WARN  i.p.c.stream.impl.ControllerImpl - gRPC call for getCurrentSegments with trace id 0 failed with server error.
io.grpc.StatusRuntimeException: INTERNAL: Stream: _requeststream State: CREATING
	at io.grpc.Status.asRuntimeException(Status.java:530)
	at io.grpc.stub.ClientCalls$StreamObserverToCallListenerAdapter.onClose(ClientCalls.java:434)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusStatsModule$StatsClientInterceptor$1$1.onClose(CensusStatsModule.java:694)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusTracingModule$TracingClientInterceptor$1$1.onClose(CensusTracingModule.java:397)
	at io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:459)
	at io.grpc.internal.ClientCallImpl.access$300(ClientCallImpl.java:63)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.close(ClientCallImpl.java:546)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.access$600(ClientCallImpl.java:467)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:584)
	at io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
	at io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:123)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:20,969 12352 [grpc-default-executor-0] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getCurrentSegments called for stream _system/_requeststream.
2020-03-19 16:38:20,989 12372 [controllerpool-62] ERROR i.p.c.s.r.g.v.ControllerServiceImpl - Controller API call with tag none failed with error: 
io.pravega.controller.store.stream.StoreException$IllegalStateException: Stream: _requeststream State: CREATING
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:99)
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:70)
	at io.pravega.controller.store.stream.PersistentStreamBase.lambda$verifyLegalState$202(PersistentStreamBase.java:1700)
	at java.util.concurrent.CompletableFuture.uniApply(CompletableFuture.java:602)
	at java.util.concurrent.CompletableFuture$UniApply.tryFire(CompletableFuture.java:577)
	at java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:474)
	at java.util.concurrent.CompletableFuture.complete(CompletableFuture.java:1962)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$getData$12(ZKStoreHelper.java:182)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$callback$30(ZKStoreHelper.java:402)
	at org.apache.curator.framework.imps.Backgrounding$1$1.run(Backgrounding.java:158)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:20,995 12378 [grpc-default-executor-1] WARN  i.p.c.stream.impl.ControllerImpl - gRPC call for getCurrentSegments with trace id 0 failed with server error.
io.grpc.StatusRuntimeException: INTERNAL: Stream: _requeststream State: CREATING
	at io.grpc.Status.asRuntimeException(Status.java:530)
	at io.grpc.stub.ClientCalls$StreamObserverToCallListenerAdapter.onClose(ClientCalls.java:434)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusStatsModule$StatsClientInterceptor$1$1.onClose(CensusStatsModule.java:694)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusTracingModule$TracingClientInterceptor$1$1.onClose(CensusTracingModule.java:397)
	at io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:459)
	at io.grpc.internal.ClientCallImpl.access$300(ClientCallImpl.java:63)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.close(ClientCallImpl.java:546)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.access$600(ClientCallImpl.java:467)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:584)
	at io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
	at io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:123)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:21,014 12397 [grpc-default-executor-1] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getCurrentSegments called for stream _system/_requeststream.
2020-03-19 16:38:21,019 12402 [controllerpool-39] ERROR i.p.c.s.r.g.v.ControllerServiceImpl - Controller API call with tag none failed with error: 
io.pravega.controller.store.stream.StoreException$IllegalStateException: Stream: _requeststream State: CREATING
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:99)
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:70)
	at io.pravega.controller.store.stream.PersistentStreamBase.lambda$verifyLegalState$202(PersistentStreamBase.java:1700)
	at java.util.concurrent.CompletableFuture.uniApply(CompletableFuture.java:602)
	at java.util.concurrent.CompletableFuture$UniApply.tryFire(CompletableFuture.java:577)
	at java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:474)
	at java.util.concurrent.CompletableFuture.complete(CompletableFuture.java:1962)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$getData$12(ZKStoreHelper.java:182)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$callback$30(ZKStoreHelper.java:402)
	at org.apache.curator.framework.imps.Backgrounding$1$1.run(Backgrounding.java:158)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:21,021 12404 [grpc-default-executor-1] WARN  i.p.c.stream.impl.ControllerImpl - gRPC call for getCurrentSegments with trace id 0 failed with server error.
io.grpc.StatusRuntimeException: INTERNAL: Stream: _requeststream State: CREATING
	at io.grpc.Status.asRuntimeException(Status.java:530)
	at io.grpc.stub.ClientCalls$StreamObserverToCallListenerAdapter.onClose(ClientCalls.java:434)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusStatsModule$StatsClientInterceptor$1$1.onClose(CensusStatsModule.java:694)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusTracingModule$TracingClientInterceptor$1$1.onClose(CensusTracingModule.java:397)
	at io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:459)
	at io.grpc.internal.ClientCallImpl.access$300(ClientCallImpl.java:63)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.close(ClientCallImpl.java:546)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.access$600(ClientCallImpl.java:467)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:584)
	at io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
	at io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:123)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:21,128 12511 [grpc-default-executor-1] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getCurrentSegments called for stream _system/_requeststream.
2020-03-19 16:38:21,132 12515 [controllerpool-67] ERROR i.p.c.s.r.g.v.ControllerServiceImpl - Controller API call with tag none failed with error: 
io.pravega.controller.store.stream.StoreException$IllegalStateException: Stream: _requeststream State: CREATING
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:99)
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:70)
	at io.pravega.controller.store.stream.PersistentStreamBase.lambda$verifyLegalState$202(PersistentStreamBase.java:1700)
	at java.util.concurrent.CompletableFuture.uniApply(CompletableFuture.java:602)
	at java.util.concurrent.CompletableFuture$UniApply.tryFire(CompletableFuture.java:577)
	at java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:474)
	at java.util.concurrent.CompletableFuture.complete(CompletableFuture.java:1962)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$getData$12(ZKStoreHelper.java:182)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$callback$30(ZKStoreHelper.java:402)
	at org.apache.curator.framework.imps.Backgrounding$1$1.run(Backgrounding.java:158)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:21,135 12518 [grpc-default-executor-1] WARN  i.p.c.stream.impl.ControllerImpl - gRPC call for getCurrentSegments with trace id 0 failed with server error.
io.grpc.StatusRuntimeException: INTERNAL: Stream: _requeststream State: CREATING
	at io.grpc.Status.asRuntimeException(Status.java:530)
	at io.grpc.stub.ClientCalls$StreamObserverToCallListenerAdapter.onClose(ClientCalls.java:434)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusStatsModule$StatsClientInterceptor$1$1.onClose(CensusStatsModule.java:694)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusTracingModule$TracingClientInterceptor$1$1.onClose(CensusTracingModule.java:397)
	at io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:459)
	at io.grpc.internal.ClientCallImpl.access$300(ClientCallImpl.java:63)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.close(ClientCallImpl.java:546)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.access$600(ClientCallImpl.java:467)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:584)
	at io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
	at io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:123)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:22,144 13527 [grpc-default-executor-1] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getCurrentSegments called for stream _system/_requeststream.
2020-03-19 16:38:22,148 13531 [controllerpool-63] ERROR i.p.c.s.r.g.v.ControllerServiceImpl - Controller API call with tag none failed with error: 
io.pravega.controller.store.stream.StoreException$IllegalStateException: Stream: _requeststream State: CREATING
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:99)
	at io.pravega.controller.store.stream.StoreException.create(StoreException.java:70)
	at io.pravega.controller.store.stream.PersistentStreamBase.lambda$verifyLegalState$202(PersistentStreamBase.java:1700)
	at java.util.concurrent.CompletableFuture.uniApply(CompletableFuture.java:602)
	at java.util.concurrent.CompletableFuture$UniApply.tryFire(CompletableFuture.java:577)
	at java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:474)
	at java.util.concurrent.CompletableFuture.complete(CompletableFuture.java:1962)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$getData$12(ZKStoreHelper.java:182)
	at io.pravega.controller.store.stream.ZKStoreHelper.lambda$callback$30(ZKStoreHelper.java:402)
	at org.apache.curator.framework.imps.Backgrounding$1$1.run(Backgrounding.java:158)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:22,150 13533 [grpc-default-executor-0] WARN  i.p.c.stream.impl.ControllerImpl - gRPC call for getCurrentSegments with trace id 0 failed with server error.
io.grpc.StatusRuntimeException: INTERNAL: Stream: _requeststream State: CREATING
	at io.grpc.Status.asRuntimeException(Status.java:530)
	at io.grpc.stub.ClientCalls$StreamObserverToCallListenerAdapter.onClose(ClientCalls.java:434)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusStatsModule$StatsClientInterceptor$1$1.onClose(CensusStatsModule.java:694)
	at io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at io.grpc.ForwardingClientCallListener$SimpleForwardingClientCallListener.onClose(ForwardingClientCallListener.java:40)
	at io.grpc.internal.CensusTracingModule$TracingClientInterceptor$1$1.onClose(CensusTracingModule.java:397)
	at io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:459)
	at io.grpc.internal.ClientCallImpl.access$300(ClientCallImpl.java:63)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.close(ClientCallImpl.java:546)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl.access$600(ClientCallImpl.java:467)
	at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:584)
	at io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
	at io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:123)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
2020-03-19 16:38:23,138 14521 [controllerpool-37] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Creating a new connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000)
2020-03-19 16:38:23,150 14533 [controllerpool-37] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 1 for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,152 14535 [controllerpool-45] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Creating a new connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000)
2020-03-19 16:38:23,153 14536 [controllerpool-45] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 6 for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,153 14536 [controllerpool-36] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Creating a new connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000)
2020-03-19 16:38:23,154 14537 [controllerpool-36] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 5 for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,157 14540 [controllerpool-63] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Creating a new connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000)
2020-03-19 16:38:23,159 14542 [controllerpool-63] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 4 for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,161 14544 [controllerpool-18] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Creating a new connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000)
2020-03-19 16:38:23,163 14546 [controllerpool-18] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 3 for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,165 14548 [controllerpool-43] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Creating a new connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000)
2020-03-19 16:38:23,168 14551 [controllerpool-43] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 2 for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,175 14558 [epollEventLoopGroup-3-3] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x6fd8fdb8]
2020-03-19 16:38:23,183 14566 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x7486633f]
2020-03-19 16:38:23,183 14566 [epollEventLoopGroup-3-4] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x9079c678]
2020-03-19 16:38:23,184 14567 [epollEventLoopGroup-3-2] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x2fff2d65]
2020-03-19 16:38:23,184 14567 [epollEventLoopGroup-3-6] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x9bebed1c]
2020-03-19 16:38:23,183 14566 [epollEventLoopGroup-3-5] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x42ecd9ed]
2020-03-19 16:38:23,195 14578 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0xe9e6df02, L:/127.0.1.1:6000 - R:/127.0.0.1:43882]
2020-03-19 16:38:23,195 14578 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x8d8c6b5e, L:/127.0.1.1:6000 - R:/127.0.0.1:43886]
2020-03-19 16:38:23,196 14579 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x5144ecd7, L:/127.0.1.1:6000 - R:/127.0.0.1:43884]
2020-03-19 16:38:23,197 14580 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0xcf63bfda, L:/127.0.1.1:6000 - R:/127.0.0.1:43888]
2020-03-19 16:38:23,197 14580 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x334f43d0, L:/127.0.1.1:6000 - R:/127.0.0.1:43890]
2020-03-19 16:38:23,199 14582 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x79bb606a, L:/127.0.1.1:6000 - R:/127.0.0.1:43892]
2020-03-19 16:38:23,200 14583 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:23,232 14615 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x5144ecd7, L:/127.0.1.1:6000 - R:/127.0.0.1:43884]
2020-03-19 16:38:23,232 14615 [epollEventLoopGroup-6-4] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0xcf63bfda, L:/127.0.1.1:6000 - R:/127.0.0.1:43888]
2020-03-19 16:38:23,233 14616 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=4294967296] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=4294967296, segment=_system/_commitStream/1.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,233 14616 [epollEventLoopGroup-6-4] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=17179869184] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=17179869184, segment=_system/_commitStream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,234 14617 [epollEventLoopGroup-6-1] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0xe9e6df02, L:/127.0.1.1:6000 - R:/127.0.0.1:43882]
2020-03-19 16:38:23,234 14617 [epollEventLoopGroup-6-1] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=21474836480] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=21474836480, segment=_system/_abortStream/1.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,237 14620 [epollEventLoopGroup-6-2] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x8d8c6b5e, L:/127.0.1.1:6000 - R:/127.0.0.1:43886]
2020-03-19 16:38:23,237 14620 [epollEventLoopGroup-6-2] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=25769803776] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=25769803776, segment=_system/_requeststream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,239 14622 [epollEventLoopGroup-6-5] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x334f43d0, L:/127.0.1.1:6000 - R:/127.0.0.1:43890]
2020-03-19 16:38:23,239 14622 [epollEventLoopGroup-6-5] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=8589934592] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=8589934592, segment=_system/_requeststream/1.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,239 14622 [epollEventLoopGroup-6-6] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x79bb606a, L:/127.0.1.1:6000 - R:/127.0.0.1:43892]
2020-03-19 16:38:23,240 14623 [epollEventLoopGroup-6-6] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=12884901888] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=12884901888, segment=_system/_abortStream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,251 14634 [epollEventLoopGroup-3-2] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,252 14635 [epollEventLoopGroup-3-3] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,253 14636 [epollEventLoopGroup-3-1] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,257 14640 [epollEventLoopGroup-3-6] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,260 14643 [epollEventLoopGroup-3-4] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,263 14646 [epollEventLoopGroup-3-5] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,285 14668 [core-18] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[3-1].
2020-03-19 16:38:23,285 14668 [core-7] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[1-1].
2020-03-19 16:38:23,288 14671 [core-4] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[2-1].
2020-03-19 16:38:23,318 14701 [epollEventLoopGroup-3-4] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=17179869184, segment=_system/_commitStream/0.#epoch.0)
2020-03-19 16:38:23,320 14703 [epollEventLoopGroup-3-2] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=25769803776, segment=_system/_requeststream/0.#epoch.0)
2020-03-19 16:38:23,321 14704 [epollEventLoopGroup-3-3] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=21474836480, segment=_system/_abortStream/1.#epoch.0)
2020-03-19 16:38:23,321 14704 [epollEventLoopGroup-3-5] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=12884901888, segment=_system/_abortStream/0.#epoch.0)
2020-03-19 16:38:23,321 14704 [epollEventLoopGroup-3-3] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 5 for endpoint 127.0.1.1
2020-03-19 16:38:23,321 14704 [epollEventLoopGroup-3-5] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 3 for endpoint 127.0.1.1
2020-03-19 16:38:23,322 14705 [epollEventLoopGroup-3-2] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 6 for endpoint 127.0.1.1
2020-03-19 16:38:23,323 14706 [epollEventLoopGroup-3-2] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_requeststream/0.#epoch.0 SegmentCreated 25769803776.
2020-03-19 16:38:23,323 14706 [epollEventLoopGroup-3-5] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_abortStream/0.#epoch.0 SegmentCreated 12884901888.
2020-03-19 16:38:23,323 14706 [epollEventLoopGroup-3-3] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_abortStream/1.#epoch.0 SegmentCreated 21474836480.
2020-03-19 16:38:23,323 14706 [epollEventLoopGroup-3-4] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 4 for endpoint 127.0.1.1
2020-03-19 16:38:23,328 14711 [epollEventLoopGroup-3-4] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_commitStream/0.#epoch.0 SegmentCreated 17179869184.
2020-03-19 16:38:23,328 14711 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=4294967296, segment=_system/_commitStream/1.#epoch.0)
2020-03-19 16:38:23,329 14712 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 1 for endpoint 127.0.1.1
2020-03-19 16:38:23,329 14712 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_commitStream/1.#epoch.0 SegmentCreated 4294967296.
2020-03-19 16:38:23,329 14712 [epollEventLoopGroup-3-6] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=8589934592, segment=_system/_requeststream/1.#epoch.0)
2020-03-19 16:38:23,330 14713 [epollEventLoopGroup-3-6] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 2 for endpoint 127.0.1.1
2020-03-19 16:38:23,330 14713 [epollEventLoopGroup-3-6] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_requeststream/1.#epoch.0 SegmentCreated 8589934592.
2020-03-19 16:38:23,344 14727 [core-5] INFO  i.p.s.server.CacheManager - CacheManager Registered ReadIndex[2-1] (_system/containers/metadata_2).
2020-03-19 16:38:23,344 14727 [core-1] INFO  i.p.s.server.CacheManager - CacheManager Registered ReadIndex[3-1] (_system/containers/metadata_3).
2020-03-19 16:38:23,344 14727 [core-20] INFO  i.p.s.server.CacheManager - CacheManager Registered ReadIndex[1-1] (_system/containers/metadata_1).
2020-03-19 16:38:23,390 14773 [core-20] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[1-1]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,390 14773 [core-10] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[3-1]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,391 14774 [core-20] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[1-1]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,391 14774 [core-10] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[3-1]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,390 14773 [core-13] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-1]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,391 14774 [core-13] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-1]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,543 14926 [controllerpool-41] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@4a52ed22, connected=java.util.concurrent.CompletableFuture@4e6d60b0[Completed normally])
2020-03-19 16:38:23,543 14926 [controllerpool-41] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 7 for endpoint 127.0.1.1. The current Channel is [id: 0x7486633f, L:/127.0.0.1:43884 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:23,546 14929 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=30064771072] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=30064771072, segment=_system/_MARK_requeststream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,554 14937 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=30064771072, segment=_system/_MARK_requeststream/0.#epoch.0)
2020-03-19 16:38:23,554 14937 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 7 for endpoint 127.0.1.1
2020-03-19 16:38:23,554 14937 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_MARK_requeststream/0.#epoch.0 SegmentCreated 30064771072.
2020-03-19 16:38:23,576 14959 [controllerpool-66] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@4a52ed22, connected=java.util.concurrent.CompletableFuture@4e6d60b0[Completed normally])
2020-03-19 16:38:23,576 14959 [controllerpool-66] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 8 for endpoint 127.0.1.1. The current Channel is [id: 0x7486633f, L:/127.0.0.1:43884 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:23,577 14960 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=34359738368] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=34359738368, segment=_system/_MARK_commitStream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,583 14966 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=34359738368, segment=_system/_MARK_commitStream/0.#epoch.0)
2020-03-19 16:38:23,583 14966 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 8 for endpoint 127.0.1.1
2020-03-19 16:38:23,583 14966 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_MARK_commitStream/0.#epoch.0 SegmentCreated 34359738368.
2020-03-19 16:38:23,601 14984 [controllerpool-18] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@4a52ed22, connected=java.util.concurrent.CompletableFuture@4e6d60b0[Completed normally])
2020-03-19 16:38:23,602 14985 [controllerpool-18] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 9 for endpoint 127.0.1.1. The current Channel is [id: 0x7486633f, L:/127.0.0.1:43884 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:23,604 14987 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=38654705664] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=38654705664, segment=_system/_MARK_abortStream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,611 14994 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=38654705664, segment=_system/_MARK_abortStream/0.#epoch.0)
2020-03-19 16:38:23,612 14995 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 9 for endpoint 127.0.1.1
2020-03-19 16:38:23,612 14995 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_MARK_abortStream/0.#epoch.0 SegmentCreated 38654705664.
2020-03-19 16:38:23,625 15008 [controllerpool-11] INFO  i.p.c.s.e.ControllerEventProcessors - Created stream _system/_requeststream
2020-03-19 16:38:23,626 15009 [controllerpool-54] INFO  i.p.c.s.e.ControllerEventProcessors - Created stream _system/_commitStream
2020-03-19 16:38:23,637 15020 [controllerpool-74] INFO  i.p.c.s.e.ControllerEventProcessors - Created stream _system/_abortStream
2020-03-19 16:38:23,637 15020 [eventprocessor-3] INFO  i.p.c.stream.impl.ClientFactoryImpl - Creating writer: 97aff98a-87d1-4ebd-a879-876442c73157 for stream: _requeststream with configuration: EventWriterConfig(initalBackoffMillis=1, maxBackoffMillis=20000, retryAttempts=10, backoffMultiple=10, enableConnectionPooling=false, transactionTimeoutTime=89999, automaticallyNoteTime=false)
2020-03-19 16:38:23,638 15021 [eventprocessor-3] INFO  i.p.c.stream.impl.SegmentSelector - Refreshing segments for stream StreamImpl(scope=_system, streamName=_requeststream)
2020-03-19 16:38:23,665 15048 [eventprocessor-3] INFO  i.p.c.stream.impl.ClientFactoryImpl - Creating writer: cbc7a8de-f31d-426e-97d5-73db0505a661 for stream: _commitStream with configuration: EventWriterConfig(initalBackoffMillis=1, maxBackoffMillis=20000, retryAttempts=10, backoffMultiple=10, enableConnectionPooling=false, transactionTimeoutTime=89999, automaticallyNoteTime=false)
2020-03-19 16:38:23,666 15049 [eventprocessor-3] INFO  i.p.c.stream.impl.SegmentSelector - Refreshing segments for stream StreamImpl(scope=_system, streamName=_commitStream)
2020-03-19 16:38:23,667 15050 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_requeststream/0.#epoch.0, writer a8d1b26b-bcae-4dbe-8ccc-997d76c6704c
2020-03-19 16:38:23,667 15050 [clientInternal-1-2] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_requeststream/1.#epoch.0, writer 931d53d4-ff01-4286-9b00-d71ebc976331
2020-03-19 16:38:23,670 15053 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_requeststream/0.#epoch.0, writerID: a8d1b26b-bcae-4dbe-8ccc-997d76c6704c
2020-03-19 16:38:23,671 15054 [clientInternal-1-3] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_requeststream/1.#epoch.0, writerID: 931d53d4-ff01-4286-9b00-d71ebc976331
2020-03-19 16:38:23,671 15054 [clientInternal-1-1] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,672 15055 [clientInternal-1-4] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_commitStream/0.#epoch.0, writer 6ad1ed6d-a091-4680-a73e-391db2f8e006
2020-03-19 16:38:23,673 15056 [eventprocessor-3] INFO  i.p.c.stream.impl.ClientFactoryImpl - Creating writer: 9c806ae4-a3ff-4dad-906c-b309fd16e1b4 for stream: _abortStream with configuration: EventWriterConfig(initalBackoffMillis=1, maxBackoffMillis=20000, retryAttempts=10, backoffMultiple=10, enableConnectionPooling=false, transactionTimeoutTime=89999, automaticallyNoteTime=false)
2020-03-19 16:38:23,673 15056 [eventprocessor-3] INFO  i.p.c.stream.impl.SegmentSelector - Refreshing segments for stream StreamImpl(scope=_system, streamName=_abortStream)
2020-03-19 16:38:23,673 15056 [clientInternal-1-4] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_commitStream/1.#epoch.0, writer b06e1bb5-67bd-4b1b-938c-4fb6c20e2a45
2020-03-19 16:38:23,674 15057 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_commitStream/0.#epoch.0, writerID: 6ad1ed6d-a091-4680-a73e-391db2f8e006
2020-03-19 16:38:23,674 15057 [clientInternal-1-4] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_commitStream/1.#epoch.0, writerID: b06e1bb5-67bd-4b1b-938c-4fb6c20e2a45
2020-03-19 16:38:23,675 15058 [clientInternal-1-1] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,675 15058 [clientInternal-1-3] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,678 15061 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0xfa5fc5ee]
2020-03-19 16:38:23,686 15069 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x30abea8f, L:/127.0.1.1:6000 - R:/127.0.0.1:43894]
2020-03-19 16:38:23,686 15069 [clientInternal-1-4] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,688 15071 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:23,683 15066 [epollEventLoopGroup-3-8] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0xe3527efc]
2020-03-19 16:38:23,697 15080 [epollEventLoopGroup-3-2] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x5061ae4e]
2020-03-19 16:38:23,697 15080 [epollEventLoopGroup-3-7] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0xaf11a85c]
2020-03-19 16:38:23,700 15083 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0xf3374c0c, L:/127.0.1.1:6000 - R:/127.0.0.1:43896]
2020-03-19 16:38:23,709 15092 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x02f0603c, L:/127.0.1.1:6000 - R:/127.0.0.1:43898]
2020-03-19 16:38:23,710 15093 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x3ce9e4fc, L:/127.0.1.1:6000 - R:/127.0.0.1:43900]
2020-03-19 16:38:23,710 15093 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:23,715 15098 [clientInternal-1-3] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_abortStream/0.#epoch.0, writer 10580dc9-9ff9-462b-afbb-e56bb18d704a
2020-03-19 16:38:23,716 15099 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_abortStream/1.#epoch.0, writer b15e305a-bb4e-4a2a-8560-919407088842
2020-03-19 16:38:23,716 15099 [epollEventLoopGroup-6-2] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x3ce9e4fc, L:/127.0.1.1:6000 - R:/127.0.0.1:43900]
2020-03-19 16:38:23,717 15100 [clientInternal-1-3] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_abortStream/0.#epoch.0, writerID: 10580dc9-9ff9-462b-afbb-e56bb18d704a
2020-03-19 16:38:23,717 15100 [clientInternal-1-3] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,719 15102 [epollEventLoopGroup-3-3] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0xc6fb6b5a]
2020-03-19 16:38:23,719 15102 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_abortStream/1.#epoch.0, writerID: b15e305a-bb4e-4a2a-8560-919407088842
2020-03-19 16:38:23,716 15099 [epollEventLoopGroup-6-1] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x02f0603c, L:/127.0.1.1:6000 - R:/127.0.0.1:43898]
2020-03-19 16:38:23,724 15107 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0xa130209c, L:/127.0.1.1:6000 - R:/127.0.0.1:43902]
2020-03-19 16:38:23,724 15107 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:23,726 15109 [clientInternal-1-1] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,726 15109 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0xa130209c, L:/127.0.1.1:6000 - R:/127.0.0.1:43902]
2020-03-19 16:38:23,729 15112 [epollEventLoopGroup-3-4] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0xcc594f19]
2020-03-19 16:38:23,731 15114 [epollEventLoopGroup-6-1] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: b06e1bb5-67bd-4b1b-938c-4fb6c20e2a45 on segment: _system/_commitStream/1.#epoch.0
2020-03-19 16:38:23,731 15114 [epollEventLoopGroup-6-2] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: a8d1b26b-bcae-4dbe-8ccc-997d76c6704c on segment: _system/_requeststream/0.#epoch.0
2020-03-19 16:38:23,731 15114 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: 10580dc9-9ff9-462b-afbb-e56bb18d704a on segment: _system/_abortStream/0.#epoch.0
2020-03-19 16:38:23,733 15116 [ControllerEventProcessors STARTING] INFO  i.p.c.s.e.ControllerEventProcessors - Starting controller event processors
2020-03-19 16:38:23,733 15116 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x00469ac4, L:/127.0.1.1:6000 - R:/127.0.0.1:43904]
2020-03-19 16:38:23,735 15118 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:23,734 15117 [epollEventLoopGroup-6-7] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x30abea8f, L:/127.0.1.1:6000 - R:/127.0.0.1:43894]
2020-03-19 16:38:23,735 15118 [epollEventLoopGroup-6-7] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: 931d53d4-ff01-4286-9b00-d71ebc976331 on segment: _system/_requeststream/1.#epoch.0
2020-03-19 16:38:23,737 15120 [epollEventLoopGroup-6-4] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x00469ac4, L:/127.0.1.1:6000 - R:/127.0.0.1:43904]
2020-03-19 16:38:23,738 15121 [epollEventLoopGroup-6-4] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: b15e305a-bb4e-4a2a-8560-919407088842 on segment: _system/_abortStream/1.#epoch.0
2020-03-19 16:38:23,741 15124 [epollEventLoopGroup-6-8] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0xf3374c0c, L:/127.0.1.1:6000 - R:/127.0.0.1:43896]
2020-03-19 16:38:23,741 15124 [epollEventLoopGroup-6-8] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: 6ad1ed6d-a091-4680-a73e-391db2f8e006 on segment: _system/_commitStream/0.#epoch.0
2020-03-19 16:38:23,744 15127 [epollEventLoopGroup-3-7] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,745 15128 [epollEventLoopGroup-3-1] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,746 15129 [epollEventLoopGroup-3-3] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,751 15134 [epollEventLoopGroup-3-8] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,753 15136 [epollEventLoopGroup-3-4] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,754 15137 [epollEventLoopGroup-3-2] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,768 15151 [ControllerEventProcessors STARTING] INFO  i.p.c.s.e.ControllerEventProcessors - Creating commit event processors
2020-03-19 16:38:23,776 15159 [core-7] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: MapStreamSegment SegmentId = 9, Name = '_system/_commitStream/0.#epoch.0', Active = 2
2020-03-19 16:38:23,776 15159 [core-7] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: MapStreamSegment SegmentId = 10, Name = '_system/_requeststream/1.#epoch.0', Active = 3
2020-03-19 16:38:23,776 15159 [ControllerEventProcessors STARTING] INFO  i.p.c.stream.impl.ClientFactoryImpl - Creating writer: e682bf1c-23f7-4a0d-bb5b-2b17c241779f for stream: _commitStream with configuration: EventWriterConfig(initalBackoffMillis=1, maxBackoffMillis=20000, retryAttempts=10, backoffMultiple=10, enableConnectionPooling=false, transactionTimeoutTime=89999, automaticallyNoteTime=false)
2020-03-19 16:38:23,777 15160 [ControllerEventProcessors STARTING] INFO  i.p.c.stream.impl.SegmentSelector - Refreshing segments for stream StreamImpl(scope=_system, streamName=_commitStream)
2020-03-19 16:38:23,779 15162 [core-2] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[3]: MapStreamSegment SegmentId = 6, Name = '_system/_abortStream/0.#epoch.0', Active = 2
2020-03-19 16:38:23,781 15164 [core-13] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: MapStreamSegment SegmentId = 11, Name = '_system/_commitStream/1.#epoch.0', Active = 4
2020-03-19 16:38:23,782 15165 [core-6] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[1]: MapStreamSegment SegmentId = 7, Name = '_system/_requeststream/0.#epoch.0', Active = 2
2020-03-19 16:38:23,785 15168 [core-4] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[1]: MapStreamSegment SegmentId = 8, Name = '_system/_abortStream/1.#epoch.0', Active = 3
2020-03-19 16:38:23,788 15171 [core-20] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[2-10].
2020-03-19 16:38:23,792 15175 [epollEventLoopGroup-3-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=47244640256, segment=_system/_requeststream/1.#epoch.0, writerId=931d53d4-ff01-4286-9b00-d71ebc976331, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,795 15178 [epollEventLoopGroup-3-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer 931d53d4-ff01-4286-9b00-d71ebc976331
2020-03-19 16:38:23,797 15180 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_commitStream/0.#epoch.0, writer 545963d3-8dfd-4dcc-85a5-1994aa05a15a
2020-03-19 16:38:23,797 15180 [clientInternal-1-2] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_commitStream/1.#epoch.0, writer 6eb930fb-eb4d-42aa-bfd6-f9a498f9a2d2
2020-03-19 16:38:23,798 15181 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_commitStream/0.#epoch.0, writerID: 545963d3-8dfd-4dcc-85a5-1994aa05a15a
2020-03-19 16:38:23,798 15181 [clientInternal-1-1] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,799 15182 [clientInternal-1-2] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_commitStream/1.#epoch.0, writerID: 6eb930fb-eb4d-42aa-bfd6-f9a498f9a2d2
2020-03-19 16:38:23,799 15182 [clientInternal-1-2] INFO  i.p.client.netty.impl.FlowHandler - Creating a new connection with flow disabled for endpoint 127.0.1.1. The current Channel is null.
2020-03-19 16:38:23,811 15194 [core-13] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[1-8].
2020-03-19 16:38:23,811 15194 [core-6] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[1-7].
2020-03-19 16:38:23,811 15194 [core-13] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[2-11].
2020-03-19 16:38:23,811 15194 [epollEventLoopGroup-3-6] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0xe1c8f14f]
2020-03-19 16:38:23,811 15194 [epollEventLoopGroup-3-5] INFO  i.p.client.netty.impl.FlowHandler - Connection established with endpoint 127.0.1.1 on channel [id: 0x042b9fdb]
2020-03-19 16:38:23,812 15195 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x98ce6767, L:/127.0.1.1:6000 - R:/127.0.0.1:43906]
2020-03-19 16:38:23,812 15195 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:23,813 15196 [core-9] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[2-9].
2020-03-19 16:38:23,813 15196 [core-7] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-10]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,813 15196 [core-7] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-10]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,813 15196 [epollEventLoopGroup-3-2] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=55834574848, segment=_system/_commitStream/1.#epoch.0, writerId=b06e1bb5-67bd-4b1b-938c-4fb6c20e2a45, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,813 15196 [epollEventLoopGroup-3-2] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer b06e1bb5-67bd-4b1b-938c-4fb6c20e2a45
2020-03-19 16:38:23,813 15196 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x175574a8, L:/127.0.1.1:6000 - R:/127.0.0.1:43908]
2020-03-19 16:38:23,817 15200 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:23,817 15200 [epollEventLoopGroup-3-8] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=51539607552, segment=_system/_commitStream/0.#epoch.0, writerId=6ad1ed6d-a091-4680-a73e-391db2f8e006, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,818 15201 [epollEventLoopGroup-3-8] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer 6ad1ed6d-a091-4680-a73e-391db2f8e006
2020-03-19 16:38:23,818 15201 [storage-io-14] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[1-7]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,818 15201 [storage-io-14] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[1-7]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,819 15202 [storage-io-15] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[1-8]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,819 15202 [storage-io-15] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[1-8]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,820 15203 [epollEventLoopGroup-3-4] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=64424509440, segment=_system/_abortStream/1.#epoch.0, writerId=b15e305a-bb4e-4a2a-8560-919407088842, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,820 15203 [epollEventLoopGroup-3-4] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer b15e305a-bb4e-4a2a-8560-919407088842
2020-03-19 16:38:23,820 15203 [epollEventLoopGroup-3-7] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=42949672960, segment=_system/_requeststream/0.#epoch.0, writerId=a8d1b26b-bcae-4dbe-8ccc-997d76c6704c, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,820 15203 [epollEventLoopGroup-3-7] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer a8d1b26b-bcae-4dbe-8ccc-997d76c6704c
2020-03-19 16:38:23,821 15204 [epollEventLoopGroup-6-5] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x98ce6767, L:/127.0.1.1:6000 - R:/127.0.0.1:43906]
2020-03-19 16:38:23,823 15206 [core-7] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-11]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,828 15211 [core-7] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-11]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,823 15206 [core-12] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[3-6].
2020-03-19 16:38:23,828 15211 [epollEventLoopGroup-6-5] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: 6eb930fb-eb4d-42aa-bfd6-f9a498f9a2d2 on segment: _system/_commitStream/1.#epoch.0
2020-03-19 16:38:23,828 15211 [epollEventLoopGroup-6-6] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x175574a8, L:/127.0.1.1:6000 - R:/127.0.0.1:43908]
2020-03-19 16:38:23,829 15212 [epollEventLoopGroup-6-6] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: 545963d3-8dfd-4dcc-85a5-1994aa05a15a on segment: _system/_commitStream/0.#epoch.0
2020-03-19 16:38:23,830 15213 [epollEventLoopGroup-3-6] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,830 15213 [epollEventLoopGroup-3-5] INFO  i.p.s.p.netty.FailingReplyProcessor - Received hello: WireCommands.Hello(type=HELLO, highVersion=9, lowVersion=5)
2020-03-19 16:38:23,831 15214 [storage-io-14] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-9]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,831 15214 [storage-io-14] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-9]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,836 15219 [epollEventLoopGroup-3-5] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=68719476736, segment=_system/_commitStream/0.#epoch.0, writerId=545963d3-8dfd-4dcc-85a5-1994aa05a15a, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,836 15219 [epollEventLoopGroup-3-6] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=73014444032, segment=_system/_commitStream/1.#epoch.0, writerId=6eb930fb-eb4d-42aa-bfd6-f9a498f9a2d2, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,836 15219 [epollEventLoopGroup-3-5] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer 545963d3-8dfd-4dcc-85a5-1994aa05a15a
2020-03-19 16:38:23,836 15219 [epollEventLoopGroup-3-6] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer 6eb930fb-eb4d-42aa-bfd6-f9a498f9a2d2
2020-03-19 16:38:23,839 15222 [epollEventLoopGroup-3-3] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=60129542144, segment=_system/_abortStream/0.#epoch.0, writerId=10580dc9-9ff9-462b-afbb-e56bb18d704a, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:23,839 15222 [epollEventLoopGroup-3-3] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer 10580dc9-9ff9-462b-afbb-e56bb18d704a
2020-03-19 16:38:23,842 15225 [core-12] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[3-6]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:23,842 15225 [core-12] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[3-6]: State changed from NotInitialized to Writing.
2020-03-19 16:38:23,861 15244 [ControllerEventProcessors STARTING] INFO  i.p.c.a.impl.ReaderGroupManagerImpl - Creating reader group: commitStreamReaders for streams: [StreamImpl(scope=_system, streamName=_commitStream)] with configuration: ReaderGroupConfig(groupRefreshTimeMillis=3000, automaticCheckpointIntervalMillis=-1, startingStreamCuts={StreamImpl(scope=_system, streamName=_commitStream)=UNBOUNDED}, endingStreamCuts={StreamImpl(scope=_system, streamName=_commitStream)=UNBOUNDED}, maxOutstandingCheckpointRequest=3)
2020-03-19 16:38:23,928 15311 [controllerpool-78] INFO  i.p.c.t.Stream.StreamMetadataTasks - _system/_RGcommitStreamReaders created in metadata store
2020-03-19 16:38:23,929 15312 [controllerpool-78] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@4a52ed22, connected=java.util.concurrent.CompletableFuture@4e6d60b0[Completed normally])
2020-03-19 16:38:23,929 15312 [controllerpool-78] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 18 for endpoint 127.0.1.1. The current Channel is [id: 0x7486633f, L:/127.0.0.1:43884 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:23,929 15312 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=77309411328] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=77309411328, segment=_system/_RGcommitStreamReaders/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:23,934 15317 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=77309411328, segment=_system/_RGcommitStreamReaders/0.#epoch.0)
2020-03-19 16:38:23,934 15317 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 18 for endpoint 127.0.1.1
2020-03-19 16:38:23,934 15317 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_RGcommitStreamReaders/0.#epoch.0 SegmentCreated 77309411328.
2020-03-19 16:38:23,998 15381 [controllerpool-76] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@4a52ed22, connected=java.util.concurrent.CompletableFuture@4e6d60b0[Completed normally])
2020-03-19 16:38:23,998 15381 [controllerpool-76] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 19 for endpoint 127.0.1.1. The current Channel is [id: 0x7486633f, L:/127.0.0.1:43884 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:23,999 15382 [epollEventLoopGroup-6-3] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=81604378624] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=81604378624, segment=_system/_MARK_RGcommitStreamReaders/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:24,003 15386 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=81604378624, segment=_system/_MARK_RGcommitStreamReaders/0.#epoch.0)
2020-03-19 16:38:24,003 15386 [epollEventLoopGroup-3-1] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 19 for endpoint 127.0.1.1
2020-03-19 16:38:24,003 15386 [epollEventLoopGroup-3-1] INFO  i.p.controller.server.SegmentHelper - CreateSegment _system/_MARK_RGcommitStreamReaders/0.#epoch.0 SegmentCreated 81604378624.
2020-03-19 16:38:24,072 15455 [ControllerEventProcessors STARTING] INFO  i.p.c.stream.impl.ClientFactoryImpl - Creating state synchronizer with stream: _RGcommitStreamReaders and configuration: SynchronizerConfig(eventWriterConfig=EventWriterConfig(initalBackoffMillis=1, maxBackoffMillis=20000, retryAttempts=10, backoffMultiple=10, enableConnectionPooling=true, transactionTimeoutTime=89999, automaticallyNoteTime=false), readBufferSize=262144)
2020-03-19 16:38:24,086 15469 [ControllerEventProcessors STARTING] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@4a52ed22, connected=java.util.concurrent.CompletableFuture@4e6d60b0[Completed normally])
2020-03-19 16:38:24,086 15469 [ControllerEventProcessors STARTING] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 20 for endpoint 127.0.1.1. The current Channel is [id: 0x7486633f, L:/127.0.0.1:43884 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:24,102 15485 [core-13] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[3]: MapStreamSegment SegmentId = 12, Name = '_system/_RGcommitStreamReaders/0.#epoch.0', Active = 3
2020-03-19 16:38:24,103 15486 [core-13] INFO  i.p.s.server.CacheManager - CacheManager Registered ReadIndex[3-12] (_system/_RGcommitStreamReaders/0.#epoch.0).
2020-03-19 16:38:24,104 15487 [clientInternal-1-4] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Fetching endpoint for segment _system/_RGcommitStreamReaders/0.#epoch.0, writer 8f6860fa-1634-4f49-bf35-2bcb893a065c
2020-03-19 16:38:24,105 15488 [clientInternal-1-1] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Establishing connection to PravegaNodeUri(endpoint=127.0.1.1, port=6000) for _system/_RGcommitStreamReaders/0.#epoch.0, writerID: 8f6860fa-1634-4f49-bf35-2bcb893a065c
2020-03-19 16:38:24,105 15488 [clientInternal-1-1] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@61590acc, connected=java.util.concurrent.CompletableFuture@7eca5f26[Completed normally])
2020-03-19 16:38:24,105 15488 [clientInternal-1-1] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 21 for endpoint 127.0.1.1. The current Channel is [id: 0x2fff2d65, L:/127.0.0.1:43886 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:24,106 15489 [epollEventLoopGroup-6-2] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: 8f6860fa-1634-4f49-bf35-2bcb893a065c on segment: _system/_RGcommitStreamReaders/0.#epoch.0
2020-03-19 16:38:24,116 15499 [core-2] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[3-12].
2020-03-19 16:38:24,117 15500 [epollEventLoopGroup-3-2] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Received appendSetup WireCommands.AppendSetup(type=APPEND_SETUP, requestId=90194313216, segment=_system/_RGcommitStreamReaders/0.#epoch.0, writerId=8f6860fa-1634-4f49-bf35-2bcb893a065c, lastEventNumber=-9223372036854775808)
2020-03-19 16:38:24,117 15500 [epollEventLoopGroup-3-2] INFO  i.p.c.s.i.SegmentOutputStreamImpl - Connection setup complete for writer 8f6860fa-1634-4f49-bf35-2bcb893a065c
2020-03-19 16:38:24,120 15503 [core-2] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[3-12]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:24,120 15503 [core-2] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[3-12]: State changed from NotInitialized to Writing.
2020-03-19 16:38:24,144 15527 [controllerpool-14] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@c6ba5b0, connected=java.util.concurrent.CompletableFuture@2933e780[Completed normally])
2020-03-19 16:38:24,144 15527 [controllerpool-14] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 22 for endpoint 127.0.1.1. The current Channel is [id: 0x6fd8fdb8, L:/127.0.0.1:43882 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:24,145 15528 [epollEventLoopGroup-6-1] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=94489280512] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=94489280512, segment=testScope/testStream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:24,149 15532 [epollEventLoopGroup-3-3] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=94489280512, segment=testScope/testStream/0.#epoch.0)
2020-03-19 16:38:24,149 15532 [epollEventLoopGroup-3-3] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 22 for endpoint 127.0.1.1
2020-03-19 16:38:24,149 15532 [epollEventLoopGroup-3-3] INFO  i.p.controller.server.SegmentHelper - [requestId=-2590570089484799806] CreateSegment testScope/testStream/0.#epoch.0 SegmentCreated 94489280512.
2020-03-19 16:38:24,220 15603 [controllerpool-66] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@c6ba5b0, connected=java.util.concurrent.CompletableFuture@2933e780[Completed normally])
2020-03-19 16:38:24,220 15603 [controllerpool-66] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 23 for endpoint 127.0.1.1. The current Channel is [id: 0x6fd8fdb8, L:/127.0.0.1:43882 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:24,221 15604 [epollEventLoopGroup-6-1] INFO  i.p.s.s.h.h.PravegaRequestProcessor - [requestId=98784247808] Creating stream segment WireCommands.CreateSegment(type=CREATE_SEGMENT, requestId=98784247808, segment=testScope/_MARKtestStream/0.#epoch.0, scaleType=0, targetRate=0).
2020-03-19 16:38:24,226 15609 [epollEventLoopGroup-3-3] INFO  i.p.controller.server.SegmentHelper - Closing connection as a result of receiving: WireCommands.SegmentCreated(type=SEGMENT_CREATED, requestId=98784247808, segment=testScope/_MARKtestStream/0.#epoch.0)
2020-03-19 16:38:24,226 15609 [epollEventLoopGroup-3-3] INFO  i.p.client.netty.impl.FlowHandler - Closing Flow 23 for endpoint 127.0.1.1
2020-03-19 16:38:24,226 15609 [epollEventLoopGroup-3-3] INFO  i.p.controller.server.SegmentHelper - [requestId=-2590570089484799806] CreateSegment testScope/_MARKtestStream/0.#epoch.0 SegmentCreated 98784247808.
2020-03-19 16:38:24,249 15632 [grpc-default-executor-0] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getCurrentSegments called for stream testScope/testStream.
2020-03-19 16:38:24,264 15647 [grpc-default-executor-0] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getCurrentSegments called for stream testScope/testStream.
2020-03-19 16:38:24,269 15652 [ControllerEventProcessors STARTING] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@c6ba5b0, connected=java.util.concurrent.CompletableFuture@2933e780[Completed normally])
2020-03-19 16:38:24,270 15653 [ControllerEventProcessors STARTING] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 24 for endpoint 127.0.1.1. The current Channel is [id: 0x6fd8fdb8, L:/127.0.0.1:43882 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:24,280 15663 [grpc-default-executor-0] INFO  i.p.c.s.r.g.v.ControllerServiceImpl - getURI called for segment testScope/testStream/0.
2020-03-19 16:38:24,290 15673 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ: [id: 0x770c48c6, L:/127.0.1.1:6000 - R:/127.0.0.1:43910]
2020-03-19 16:38:24,291 15674 [epollEventLoopGroup-5-1] INFO  i.n.handler.logging.LoggingHandler - [id: 0xb841d9c0, L:/127.0.1.1:6000] READ COMPLETE
2020-03-19 16:38:24,293 15676 [epollEventLoopGroup-6-7] INFO  i.p.s.s.h.handler.AppendProcessor - Received hello from connection: [id: 0x770c48c6, L:/127.0.1.1:6000 - R:/127.0.0.1:43910]
2020-03-19 16:38:24,294 15677 [epollEventLoopGroup-6-7] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: 1ac8d7db-8d0a-450a-a18a-d43ea485a966 on segment: testScope/testStream/0.#epoch.0
2020-03-19 16:38:24,297 15680 [core-1] INFO  i.p.s.s.c.StreamSegmentContainerMetadata - SegmentContainer[2]: MapStreamSegment SegmentId = 21, Name = 'testScope/testStream/0.#epoch.0', Active = 5
2020-03-19 16:38:24,300 15683 [core-4] INFO  i.p.s.server.CacheManager - CacheManager Registered AttributeIndex[2-21].
2020-03-19 16:38:24,305 15688 [core-1] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-21]: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.
2020-03-19 16:38:24,305 15688 [core-1] INFO  i.p.s.s.writer.SegmentAggregator - StorageWriter[2-21]: State changed from NotInitialized to Writing.
2020-03-19 16:38:24,306 15689 [ControllerEventProcessors STARTING] INFO  i.p.c.netty.impl.ConnectionPoolImpl - Reusing connection: Connection(uri=PravegaNodeUri(endpoint=127.0.1.1, port=6000), flowHandler=io.pravega.client.netty.impl.FlowHandler@35415be6, connected=java.util.concurrent.CompletableFuture@707274d3[Completed normally])
2020-03-19 16:38:24,307 15690 [ControllerEventProcessors STARTING] INFO  i.p.client.netty.impl.FlowHandler - Creating Flow 25 for endpoint 127.0.1.1. The current Channel is [id: 0x9079c678, L:/127.0.0.1:43888 - R:127.0.1.1/127.0.1.1:6000].
2020-03-19 16:38:24,308 15691 [epollEventLoopGroup-6-4] INFO  i.p.s.s.h.handler.AppendProcessor - Setting up appends for writer: ddaae205-b069-44e6-bc10-2b24e2bb39f7 on segment: _system/_RGcommitStreamReaders/0.#epoch.0
2020-03-19 16:38:24,314 15697 [core-6] INFO  i.p.s.server.CacheManager - CacheManager Registered ReadIndex[2-21] (testScope/testStream/0.#epoch.0).
test tests::test_event_stream_writer ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

