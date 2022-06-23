FROM gradle:7.4.1-jdk17 as builder
WORKDIR /usr/src/app/
COPY --chown=gradle:gradle build.gradle gradle.properties settings.gradle ./
COPY --chown=gradle:gradle ./src ./src
# ENV GRADLE_USER_HOME=/usr/src/app/
ARG GIT_BRANCH
ARG GIT_COMMIT
ARG GIT_COMMIT_DATE
ARG BUILD_DATE
RUN echo "GIT_BRANCH: $GIT_BRANCH" \
  && echo "GIT_COMMIT: $GIT_COMMIT" \
  && echo "GIT_COMMIT_DATE: $GIT_COMMIT_DATE" \
  && echo "BUILD_DATE: $BUILD_DATE" \
  && gradle -v \
  && gradle --no-daemon testClasses assemble -PGIT_BRANCH="$GIT_BRANCH" -PGIT_COMMIT="$GIT_COMMIT" -PGIT_COMMIT_DATE="$GIT_COMMIT_DATE" -PBUILD_DATE="$BUILD_DATE" --gradle-user-home /usr/src/app/ --info

FROM gradle:7.4.1-jdk17 as tester
WORKDIR /usr/src/app/
COPY --from=builder --chown=gradle:gradle /usr/src/app /usr/src/app
COPY --chown=gradle:gradle conf-test.json ./
COPY --chown=gradle:gradle role-permissions-test.json ./
RUN gradle --no-daemon test -PGIT_BRANCH="$GIT_BRANCH" -PGIT_COMMIT="$GIT_COMMIT" -PGIT_COMMIT_DATE="$GIT_COMMIT_DATE" -PBUILD_DATE="$BUILD_DATE" --gradle-user-home /usr/src/app/ --info

FROM openjdk:17-jdk-alpine as prod
WORKDIR /usr/src/app/
COPY --from=builder --chown=campudus:campudus ./build/libs/grud-backend-0.1.0-fat.jar ./tableaux-fat.jar
# USER campudus
CMD [ "java", "-Xmx512M", "-jar", "./tableaux-fat.jar", "-conf /config.json" ]
