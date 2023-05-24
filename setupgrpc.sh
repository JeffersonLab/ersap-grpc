#!/bin/sh

# source this script:
#    source setupgrpc

setenv GRPC_INSTALL_DIR /daqfs/gRPC/installation
#setenv GRPC_INSTALL_DIR /Users/timmer/coda/grpc_installation

# Want to pick up the correct protoc - the one installed with grpc
setenv PATH $GRPC_INSTALL_DIR/bin:$PATH

# for Mac
#setenv DYLD_LIBRARY_PATH $GRPC_INSTALL_DIR/lib:$DYLD_LIBRARY_PATH
# for Linux
setenv LD_LIBRARY_PATH $GRPC_INSTALL_DIR/lib:$LD_LIBRARY_PATH

