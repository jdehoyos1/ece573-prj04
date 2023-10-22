#!/bin/bash

docker build -t ece473-prj04-writer:v1 .
kind load docker-image ece473-prj04-writer:v1
