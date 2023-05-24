#!/bin/sh

# source this script:
#    source setupgrpc

export GRPC_INSTALL_DIR=/daqfs/gRPC/installation
#export GRPC_INSTALL_DIR=/Users/timmer/coda/grpc_installation

# Want to pick up the correct protoc - the one installed with grpc
export PATH=$GRPC_INSTALL_DIR/bin:$PATH

# for Mac
#export DYLD_LIBRARY_PATH=$GRPC_INSTALL_DIR/lib:$DYLD_LIBRARY_PATH
# for Linux
export LD_LIBRARY_PATH=$GRPC_INSTALL_DIR/lib:$LD_LIBRARY_PATH

