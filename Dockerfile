FROM openjdk:jre-alpine

COPY target/tabby-0.1.0-SNAPSHOT-standalone.jar /

ENTRYPOINT ["java", "-jar", "/tabby.jar"]
