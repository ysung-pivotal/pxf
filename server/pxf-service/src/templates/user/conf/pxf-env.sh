#!/bin/bash

##############################################################################
# This file contains PXF properties that can be specified by users           #
# to customize their deployments. This file is sourced by PXF Server control #
# scripts upon initialization, start and stop of the PXF Server.             #
#                                                                            #
# To update a property, uncomment the line and provide a new value.          #
##############################################################################

PXF_CONF="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# Path to JAVA
# export JAVA_HOME=/usr/java/default

# Path to Log directory
# export PXF_LOGDIR="${PXF_CONF}/logs"

# Memory
# export PXF_JVM_OPTS="-Xmx2g -Xms1g"

# Kerberos path to keytab file owned by pxf service with permissions 0400
# export PXF_KEYTAB="${PXF_CONF}/keytabs/pxf.service.keytab"

# Kerberos principal pxf service should use. _HOST is replaced automatically with hostnames FQDN
# export PXF_PRINCIPAL="gpadmin/_HOST@EXAMPLE.COM"

# End-user identity impersonation, set to true to enable
# export PXF_USER_IMPERSONATION=true
