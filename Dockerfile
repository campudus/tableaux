FROM openjdk:17-alpine as gradle-cache
WORKDIR /usr/src/app/
ENV GRADLE_USER_HOME /cache
COPY build.gradle gradle.properties settings.gradle gradlew ./
COPY ./gradle ./gradle
RUN ./gradlew wrapper
RUN ./gradlew --no-daemon testClasses assemble \
  # prevent errors b/c in caching stage we have no source files yet
  -x spotlessScala \ 
  --project-cache-dir /cache \
  --info

FROM openjdk:17-alpine as builder
WORKDIR /usr/src/app/
ENV GRADLE_USER_HOME /cache
COPY --from=gradle-cache /cache /cache
COPY ./gradle ./gradle
COPY build.gradle gradle.properties settings.gradle gradlew ./
# COPY --from=gradle-cache /usr/src/app/ /usr/src/app/
# COPY --from=cache /cache /home/gradle/.gradle
# COPY --from=gradle-cache /usr/src/app/.gradle /usr/src/app/.gradle
# RUN ls -rtla /cache
# RUN ls -rtla /usr/src/app/
# COPY --from=gradle-cache  /usr/src/app /usr/src/app
# COPY build.gradle gradle.properties settings.gradle ./
COPY ./src ./src
ARG GIT_BRANCH
ARG GIT_COMMIT
ARG GIT_COMMIT_DATE
ARG BUILD_DATE
RUN echo "GIT_BRANCH: $GIT_BRANCH" \
  && echo "GIT_COMMIT: $GIT_COMMIT" \
  && echo "GIT_COMMIT_DATE: $GIT_COMMIT_DATE" \
  && echo "BUILD_DATE: $BUILD_DATE" \
  && ./gradlew --no-daemon testClasses assemble \
  -PGIT_BRANCH="$GIT_BRANCH" \
  -PGIT_COMMIT="$GIT_COMMIT" \
  -PGIT_COMMIT_DATE="$GIT_COMMIT_DATE" \
  -PBUILD_DATE="$BUILD_DATE" \
  --project-cache-dir /cache \
  --info

FROM openjdk:17-alpine as tester
WORKDIR /usr/src/app/
COPY --from=gradle-builder /usr/src/app /usr/src/app
COPY --from=builder /usr/src/app /usr/src/app
COPY conf-test.json ./
COPY role-permissions-test.json ./
RUN ./gradlew --no-daemon test \
  -PGIT_BRANCH="$GIT_BRANCH" \
  -PGIT_COMMIT="$GIT_COMMIT" \
  -PGIT_COMMIT_DATE="$GIT_COMMIT_DATE" \
  -PBUILD_DATE="$BUILD_DATE" \
  --info

FROM openjdk:17-jdk-alpine as prod
WORKDIR /usr/src/app/
COPY --from=builder ./build/libs/grud-backend-0.1.0-fat.jar ./tableaux-fat.jar
# USER campudus
CMD [ "java", "-Xmx512M", "-jar", "./tableaux-fat.jar", "-conf /config.json" ]
