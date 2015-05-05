#!/bin/sh

JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8888"
export JAVA_OPTS

sbt runMod