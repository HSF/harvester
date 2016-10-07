# Harvester

[TOC]

### Introduction

Harvester is a resource-facing service between the PanDA server and collection of pilots. It is a lightweight stateless service running on a VObox or an edge node of HPC centers to provide a uniform view for various resources.

----------

### Installation
> Harvester can be installed with or without  root privilege.
>
#### Without root privilege
```
$ virtualenv harvester
$ cd harvester
$ . bin/activate
$ pip install git+git://github.com/PanDAWMS/panda-harvester.git
```
#### With root privilege
