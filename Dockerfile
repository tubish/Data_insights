ARG PYTHON_VERSION=3.7.10-slim-buster

FROM python:${PYTHON_VERSION}
LABEL maintainer="twocircles"

WORKDIR /code

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
ENV PYTHONPATH "/usr/bin/python3"
ENV IVY_DIRECTORY "/opt/spark/work-dir/.ivy2/"

# Install OpenJDK-11
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

# Copy Code
COPY . .

# Install PYTHON requirements
RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash"]
