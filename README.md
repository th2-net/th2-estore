# Overview

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
