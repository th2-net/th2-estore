# Overview (2.4.0)

Event store (estore) is an important th2 component responsible for storing events into Cradle. Please refer to [Cradle repository] (https://github.com/th2-net/cradleapi/blob/master/README.md) for more details. This component has a pin for listening events via MQ.

The infra-operator adds the special MQ pin with the "event" and "publish" attributes , to every box described via infra-schema.

Event is a base entity of th2. Information related to the work of every component, the executed tests, and the problems that happened are presented as events hierarchy.
Every event contains important parts:
* id - unique identifier within th2
* parent id - optional link to parent event
* description - set of fields for short description
* body - useful data in JSON format

# Custom resources for infra-mgr

Infra schema can only contain one estore box description. It consists of one required option - docker image . Pin configuration is generated and managed by infra-operator.

General view of the component will look like this:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Estore
metadata:
  name: estore
spec:
  image-name: ghcr.io/th2-net/th2-estore
  image-version: <image version>
  extended-settings:
    service:
      enabled: false
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError -Ddatastax-java-driver.advanced.connection.init-query-timeout=\"5000 milliseconds\""
    resources:
      limits:
        memory: 500Mi
        cpu: 200m
      requests:
        memory: 100Mi
        cpu: 20m
```

# Common features

This is a list of supported features provided by libraries.
1. CradleMaxEventBatchSize - this option defines the maximum event batch size in bytes.
Please see more details about this feature via [link](https://github.com/th2-net/th2-common-j#configuration-formats)

