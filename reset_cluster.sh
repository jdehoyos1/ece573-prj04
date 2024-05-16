#!/bin/bash

docker pull cassandra:4.1.3
kind delete cluster
kind create cluster --config cluster.yml
kind load docker-image cassandra:4.1.3
