# to use APP_HOME in other stages, we need to redeclare it in each stage (not on docker for mac)
ARG APP_HOME=/usr/src/app

FROM gradle:7.4.1-jdk17 as cacher
ARG APP_HOME
ENV GRADLE_USER_HOME /cache
WORKDIR $APP_HOME
COPY build.gradle gradle.properties settings.gradle ./
# prevent errors from spotless b/c in cacher stage we have no source files yet
RUN gradle testClasses assemble -x spotlessScala

FROM cacher as builder
ARG APP_HOME
ENV GRADLE_USER_HOME /cache
WORKDIR $APP_HOME
COPY --from=cacher /cache /cache
# prevent from strange docker error when built without buildkit, see: https://github.com/moby/moby/issues/37965
RUN true 
COPY --chown=gradle:gradle . $APP_HOME
RUN gradle -v \
  && gradle testClasses assemble \
  && mv conf-jenkins.json conf-test.json

FROM eclipse-temurin:25-alpine as prod
ARG APP_HOME
WORKDIR $APP_HOME
COPY --from=builder $APP_HOME/build/libs/grud-backend-0.1.0-fat.jar ./tableaux-fat.jar
CMD [ "java", "-Xmx512M", "-jar", "./tableaux-fat.jar", "-conf", "/config.json" ]
