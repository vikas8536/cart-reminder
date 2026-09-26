# syntax=docker/dockerfile:1.7

# Build stage: the Gradle image's own gradle 8.10.2 (same as the wrapper), dependency cache in a BuildKit mount.
FROM gradle:8.10.2-jdk21 AS build
ENV GRADLE_USER_HOME=/gradle-cache
WORKDIR /src
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY src ./src
RUN --mount=type=cache,target=/gradle-cache \
    gradle --no-daemon --console=plain installDist -x test

# Runtime stage: JRE only. The image ships wget, which the compose healthcheck uses.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/install/abandoned-cart-recovery/ /app/
ENTRYPOINT ["/app/bin/abandoned-cart-recovery"]
