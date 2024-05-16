#!/bin/bash

docker build -t ece573-prj04-writer:v1 .
kind load docker-image ece573-prj04-writer:v1
