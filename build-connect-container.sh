#!/bin/bash
mvn clean install -DskipTests -DskipITs
docker build -t flow/connect:1.1 .