FROM gradle:7.4.1-jdk17 as cacher
ENV APP_HOME=/usr/src/app
ENV GRADLE_USER_HOME /cache
WORKDIR $APP_HOME
COPY build.gradle gradle.properties settings.gradle ./
RUN gradle --no-daemon testClasses assemble \
  # prevent errors from spotless b/c in cacher stage we have no source files yet
  -x spotlessScala

FROM gradle:7.4.1-jdk17 as builder
ENV GRADLE_USER_HOME /cache
WORKDIR $APP_HOME
COPY --from=cacher /cache /cache
COPY --chown=gradle:gradle . $APP_HOME
RUN gradle -v \
  && gradle testClasses assemble \
  && mv conf-jenkins.json conf-test.json

FROM openjdk:22-slim as prod
WORKDIR $APP_HOME
COPY --from=builder --chown=campudus:campudus ./build/libs/grud-backend-0.1.0-fat.jar ./tableaux-fat.jar
# USER campudus
CMD [ "java", "-Xmx512M", "-jar", "./tableaux-fat.jar", "-conf /config.json" ]
