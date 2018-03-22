# Eunomia - The Elasticsearch Request Prioritizer

Eunomia is a minor Greek god of lawfulness and order. 

## Overview 

This plugin attempts to prioritize requests on an Elasticsearch node based on a number of factors: requested priority,
a priority group, how long as the request been waiting, how many active requests are running, what priority groups are
currently using the most resources. At the core, Eunomia uses a fair, tiered, burstable, deadlining, queue. 

## Features

The following are some of the high level features that Eunomia offers (see below for details):

* Tunable Request Priority Queue 
* Priority group size policy
* Highly Configurable
* New `_cat/prioritized_action` endpoint for tracking requests

## Install

Current supported versions:

| ES Version   | Plugin URL                                                                                     |
| ------------ | -----------------------------------------------------------------------------------------------|
| 2.3.2        | https://github.com/sproutsocial/eunomia/releases/download/v1.0.0-ES2.3.2/eunomia-1.0.0-ES2.3.2.zip

First install the plugin on all nodes in the cluster. Then add `transport.service.type: prioritizing-transport-service`
to the `elasticsearch.yml` file. Restart all nodes. 

## Configuration

All configuration options are dynamic. 

| Option                                          | Default   | Description                                                      
| ----------------------------------------------- | --------- | ---------------------------------------------------------------  
| eunomia.requestPrioritizing.enabled             | true      | Should requests be prioritized                                   
| eunomia.groupThrottling.enabled                 | true      | Should group throttling be enabled                               
| eunomia.groupThrottling.maxGroupSize            | 1000      | Maximum allowed queued requests from a particular group          
| eunomia.priorityDispatcher.targetActiveRequests | #CPUs - 2 | Number of active requests to optimize for                        
| eunomia.priorityDispatcher.deadlineInSeconds    | 10        | Number of seconds a request can be queued before it is "promoted"

