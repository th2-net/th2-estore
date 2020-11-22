# Overview

Event store  (estore) is an important th2 component which stores events into Cradle. See [Cradle repository](https://github.com/th2-net/cradleapi/blob/master/README.md) for more detail. This component has a pin for listening events via MQ. 

Infra-operator adds the special mq pin with "event", "publish" attributes to every box described via infra-schema.

Event is a base entity of th2. Information related to work every component, executed tests, troubles are presented as events hierarchy. 
Every event has got impotant parts:
* id - unique identifier within th2
* parent id - optional link to parent event
* description - set of fields for short description
* body - useful data in JSON format

# Custom resources for infra-mgr

Infra schema can have only one estore box description. It consists of one required option - docker image, pin configuration is generated and managed by infra-operator.
