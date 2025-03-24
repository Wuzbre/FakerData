FROM openjdk:8-jdk-slim

# Arguments
ARG APP_HOME=/app
ARG USER_ID=1000
ARG GROUP_ID=1000

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    procps \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Maven
ENV MAVEN_VERSION=3.8.6
ENV MAVEN_HOME=/usr/share/maven
RUN mkdir -p $MAVEN_HOME \
    && curl -fsSL https://dlcdn.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz \
    | tar -xzC /usr/share/maven --strip-components=1 \
    && ln -s $MAVEN_HOME/bin/mvn /usr/bin/mvn

# Create app directory
RUN mkdir -p $APP_HOME
WORKDIR $APP_HOME

# Copy project files
COPY . $APP_HOME/

# Set proper permissions
RUN chmod +x $APP_HOME/build.sh $APP_HOME/run-etl.sh

# Build the application
RUN $APP_HOME/build.sh

# Default command
CMD ["./run-etl.sh"] 