FROM openjdk:8-alpine

COPY target/uberjar/bitool.jar /bitool/app.jar

EXPOSE 3000

CMD ["java", "-jar", "/bitool/app.jar"]
