FROM openjdk:jre-alpine

COPY target/tabby-0.1.0-SNAPSHOT-standalone.jar /tabby.jar

ENTRYPOINT ["java", "-jar", "/tabby.jar"]
