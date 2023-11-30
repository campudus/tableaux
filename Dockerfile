FROM gradle:7.4.1-jdk17 as builder
ENV APP_HOME=/usr/src/app
WORKDIR $APP_HOME
COPY --chown=gradle:gradle . $APP_HOME
# RUN ls -rtla $APP_HOME
RUN gradle -v \
  && gradle testClasses assemble \
  && mv conf-jenkins.json conf-test.json

FROM gradle:7.4.1-jdk17 as tester
WORKDIR $APP_HOME
COPY --from=builder . .
RUN gradle test

FROM openjdk:22-slim as prod
WORKDIR $APP_HOME
COPY --from=builder --chown=campudus:campudus ./build/libs/grud-backend-0.1.0-fat.jar ./tableaux-fat.jar
# USER campudus
CMD [ "java", "-Xmx512M", "-jar", "./tableaux-fat.jar", "-conf /config.json" ]
