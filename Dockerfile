FROM registry.access.redhat.com/ubi9/openjdk-21:1.23 AS builder
WORKDIR /application
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} application.jar
# Extraemos el contenido del jar para aprovechar el sistema de capas de Docker
RUN java -Djarmode=layertools -jar application.jar extract

# Stage 2: Imagen final
FROM registry.access.redhat.com/ubi9/openjdk-21:1.23
LABEL org.opencontainers.image.source https://github.com/buildspace-run/demo-microservices-crypto-exchange-persister
WORKDIR /application
# Copiamos las capas extraídas del builder
COPY --from=builder /application/dependencies/ ./
COPY --from=builder /application/spring-boot-loader/ ./
COPY --from=builder /application/snapshot-dependencies/ ./
COPY --from=builder /application/application/ ./

# Usuario estándar de las imágenes de Red Hat (no-root)
USER 185
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
