# pydatahub

## Purpose

A framework to define / store / operate on data

The initial idea of this repository comes from k8s (https://github.com/kubernetes/kubernetes). The data model of api server shocks me and I thought it could be a general data modeling / storing and requesting / responding approach. I wanna implement a framework like that in python.

So, I'm intending to solve the following things:

* A general way to do data modeling. The package 'schematics' does it well in the past time, but it's designed for relational database and lacks of some modern features such as:
  * Avoid generate none property by default
  * Specify the property could not be updated (TODO: Add RFC data updating definitions)
  * Support dynamic / any type (It's somehow very common to represent data in this way by JSON)
* A flexible way to build up a data processing pipeline. I was inspired by 'admission control' of k8s when I'm thinking about how can I generalize the operation on the data models. So, the it should have the following features:
  * A clear interface: Well defined operations and standard pluggable processor framework.
  * Simple and easy to use: Just run the processor in the same linear order as they're defined.
  * Fully configurable, the pipeline could be generated on the fly.
* An adaptive storage interface. The underlying storage approach is not defined but the storage interface. So, it should be very easy to extend. To achieve this, the interface should be:
  * As small as possible. Only atomic methods / operations are defined.
  * Avoid exposing the details of actual storage infrastructure. For those implementation-dependent feature, I defined a couple of features (e.g. Expire feature / Query feature / KV Query feature ...), and the storage adapters which implements the interface should clearly define which feature it supports. And also, each feature type will have a standard interface to apply this feature.
* Query schema definition. It's quite important to query data by a couple of conditions besides simple CRUD operations. But it brings complexities and implementation-dependent features as the above has told.
* Data hook / watch supports. It's quite trick to implement data change hook / watch features at the application level. We'll lose all data modifications directly to the underlying storage infrastructure (e.g. write database directly) and it could not capture data modifications in the distribution environment (Deploy multiple instances of data hub application). We'll have further discussion about this topic in the data hook / watch modules.
* Standard restful interface via web. This is an optional approach to expose system features but it's recommended to implement it in the restful way. It supports:
  * Basic features of restful web service
  * Update feature by extending restful model
  * Request proxy / wrapper.

## Overview

The system is consist of the following modules:

* DataRepository. This is the abstract interface of all storage infrastructures.
* OperationController. This is the data processor on a specified operation.
* DataManager. This is the object level data interface which provides query / get / replace .. operations on data, managing operation controller, and support data hook / watch features.
* RestfulWebInterface. This is the api level data interface which provides restful style web service.
* DataHookManager. This is the hook manager implements hook registration / trigger etc..

## The restful model

__Conceptions__:

* Resource, maps to a data model type. The name of resource could not be started with '\_'
* Path
* Methods
  * GET
  * POST
  * PUT
  * DELETE
  * PATCH
    RFC: https://tools.ietf.org/html/rfc5789

__Operations__:

* Query
  * The restful way
    Method: GET
    Path: /{Resource name}
    Parameters:
    * start
    * size
    * query
  * The extended way
    Method: POST
    Path: /{Resource name}/\_query
    Parameters: none
    Payload:
    {
        start
        size
        query
        features
    }
* Get One
  * The restful way
  * The extended way
* Create One
  *
* Replace One
* Update One
* Delete One
