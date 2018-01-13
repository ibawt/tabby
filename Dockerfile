FROM clojure
WORKDIR /usr/src/myapp
COPY project.clj /usr/src/myapp
RUN lein deps
COPY . /usr/src/myapp
RUN mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" tabby-standalone.jar

FROM openjdk:jre-alpine
RUN mkdir -p /opt/tabby
WORKDIR /opt/tabby
COPY --from=0 /usr/src/myapp/tabby-standalone.jar tabby.jar
ENTRYPOINT ["java", "-jar", "tabby.jar"]
