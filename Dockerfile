ARG APP_HOME=/usr/src/app/

FROM gradle:7.4.1-jdk17 as builder
WORKDIR $APP_HOME
COPY --chown=gradle:gradle . $APP_HOME
RUN gradle -v \
  && gradle --no-daemon testClasses assemble

FROM gradle:7.4.1-jdk17 as tester
WORKDIR $APP_HOME
COPY --from=builder --chown=gradle:gradle /home/gradle /home/gradle
COPY --chown=gradle:gradle conf-test.json $APP_HOME
COPY --chown=gradle:gradle role-permissions-test.json $APP_HOME

RUN pwd && ls -rtl
RUN gradle --no-daemon test --info

FROM openjdk:17-jdk-alpine as prod
WORKDIR $APP_HOME
COPY --from=builder --chown=campudus:campudus ./build/libs/grud-backend-0.1.0-fat.jar ./tableaux-fat.jar
# USER campudus
CMD [ "java", "-Xmx512M", "-jar", "./tableaux-fat.jar", "-conf /config.json" ]
