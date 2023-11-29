FROM openjdk:22-slim

WORKDIR /usr/src/app

COPY ./build/libs/grud-backend-0.1.0-fat.jar ./tableaux-fat.jar

CMD [ "java", "-Xmx512M", "-jar", "./tableaux-fat.jar", "-conf /config.json" ]
