# Overview (5.7.0)

Event store (estore) is an important th2 component responsible for storing events into Cradle. Please refer to [Cradle repository] (https://github.com/th2-net/cradleapi/blob/master/README.md) for more details. This component has a pin for listening events via MQ.

The infra-operator adds the special MQ pin with the "event" and "publish" attributes , to every box described via infra-schema.

Event is a base entity of th2. Information related to the work of every component, the executed tests, and the problems that happened are presented as events hierarchy.
Every event contains important parts:
* book - name of the book
* id - unique identifier within th2
* parent id - optional link to parent event
* description - set of fields for short description
* body - useful data in JSON format
* attachedMessageIDs - the list of message IDs that are linked to the event

# Custom resources for infra-mgr

Infra schema can only contain one estore box description. It consists of one required option - docker image . Pin configuration is generated and managed by infra-operator.

General view of the component will look like this:
```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Estore
metadata:
  name: estore
spec:
  imageName: ghcr.io/th2-net/th2-estore
  imageVersion: <image version>
  customConfig:
    maxTaskCount: 128
    maxTaskDataSize: 536870912
    maxRetryCount: 3
  mqRouter:
    prefetchCount: 100
  extendedSettings:
    envVariables:
      JAVA_TOOL_OPTIONS: >
        -XX:+ExitOnOutOfMemoryError
        -XX:+UseContainerSupport
        -Dlog4j2.shutdownHookEnabled=false
        -XX:MaxRAMPercentage=84.2
        -XX:MaxMetaspaceSize=70M
        -XX:CompressedClassSpaceSize=10M
        -XX:ReservedCodeCacheSize=40M
        -XX:MaxDirectMemorySize=50M
        -Ddatastax-java-driver.advanced.connection.init-query-timeout="5000 milliseconds"
        -Ddatastax-java-driver.basic.request.timeout="3 seconds"
    resources:
      limits:
        memory: 2000Mi
        cpu: 2500m
      requests:
        memory: 100Mi
        cpu: 500m
```

# Configuration

Configuration is provided as `custom.json` file

```json
{
    "maxTaskCount": 256,
    "maxTaskDataSize": 133169152,
    "maxRetryCount": 1000000,
    "retryDelayBase": 5000,
    "processingThreads": 4
}
```


+ _maxTaskCount_ - maximum number of events that will be processed simultaneously (default: 256)
+ _maxTaskDataSize_ - maximum total data size of events during parallel processing (default: half of available memory)
+ _maxRetryCount_ - maximum number of retries that will be done in case of event persistence failure (default: 1000000)
+ _retryDelayBase_ - constant that will be used to calculate next retry time(ms) (default: 5000):
retryDelayBase * retryNumber
+ _processingThreads_ - number of task processing threads (default: number available logical cpu cores)

If some of these parameters are not provided, estore will use default(undocumented) value.
If _maxTaskCount_ or _maxTaskDataSize_ limits are reached during processing, estore will pause processing new events 
until some events are processed

# Common features

This is a list of supported features provided by libraries.

_CradleMaxEventBatchSize_ - this option defines the maximum event batch size in bytes.
Please see more details about this feature via [link](https://github.com/th2-net/th2-common-j#configuration-formats)

# Performance

The component provides a performance of 100K events per second if the events are packaged in batches of 20 or
more events(event size: 1.4KB, event status: SUCCEED, attached messages: 1).

Processing speed (K events/sec) vs batch size for estore (under load of 100K events/s):

![performance chart](./perf_chart.png)

Note: for smaller batches (less than 100 events) higher mqRouter.prefetchCount value should be used (e.g. 1000) to achieve these results.

# Changes

## 5.7.0

* Using separate executor instead of ForkJoinPool.commonPool() when storing events
* Updated cradle api: `5.4.0-dev`

## 5.6.0

* Migrated to th2 gradle plugin `0.0.8`
* Updated bom: `4.6.1`
* Updated common: `5.12.0-dev`
* Updated common-utils: `2.2.3-dev`
* Updated cradle api: `5.3.0-dev`

## 5.5.0

* Updated bom: `4.6.0`
* Updated common: `5.9.1-dev`

## 5.4.0

* Updated cradle api: `5.2.0-dev`
* Updated common: `5.8.0-dev`

## 5.3.0

* Estore persists event with aggregated statistics about internal errors into cradle periodically
* Updated common: `5.6.0-dev`
* Added common-utils: `2.2.2-dev`

## 5.2.2

* Migrated to the cradle version with fixed load pages where `removed` field is null problem.
* Updated cradle: `5.1.4-dev`

## 5.2.1

* Fixed the lost messages problem when estore is restarted under a load

* Updated common: `5.4.2-dev`

## 5.2.0
* Updated bom: `4.5.0-dev`
* Updated common: `5.4.0-dev`

## 5.1.0

+ Updated cradle 5.1.1-dev
+ Updated common 5.3.0-dev

## 5.0.0

+ Migration to books/pages cradle 5.0.0

## 4.1.1

+ Using Cradle 3.1.4 with a fix for degraded performance while persisting events
+ Sending manual acknowledgements only after successfully saving event
+ Changed default configuration to 
```json
{
    "maxTaskCount" : 256,
    "maxRetryCount" : 1000000,
    "retryDelayBase" : 5000
}
```
## 4.1.0

+ Added metrics collection for Prometheus
+ Limiting simultaneously processed events by number and content size  
+ Retrying saving events in case of failure  
+ Updated cradle version from `3.1.1` to `3.1.3`. This requires data migration

## 4.0.0

+ Update common version from `3.35.0` to `3.39.3`
  + Fixed problem with unconfirmed deliveries 
  + Cradle version is updated from `2.21.0` to `3.1.1`
    (please, note that the new version of Cradle is not compatible with Cradle `2.*` and requires additional schema migration if you want to keep the stored data)
  + Messages' IDs that are attached to the event are stored along with event itself

## 3.7.0

+ Update common version from `3.31.6` to `3.35.0`
+ Update Cradle version from `2.20.2` to `2.21.0`

## 3.6.0

+ Update common version from `3.30.0` to `3.31.6`
+ Update Cradle version from `2.20.0` to `2.20.2`

## 3.5.1

+ Added util methods from store-common
+ Removed dependency to store-common 

## 3.5.0

+ Update common version from `3.18.0` to `3.29.0`
+ Update store-common version from `3.1.0` to `3.2.0`

## 3.4.0

### Changed:

+ Disable waiting for connection recovery when closing the `SubscribeMonitor`
+ Update Cradle version from `2.9.1` to `2.13.0`
+ Rework logging for incoming and outgoing messages
+ Resets embedded log4j configuration before configuring it from a file

## 3.2.0

+ Compressed metadata for events

## 3.1.0

+ Use async methods for storing events to the Cradle