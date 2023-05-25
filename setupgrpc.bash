#!/bin/bash

# source this script:
#    source setupgrpc.bash

if test -n "${GRPC_INSTALL_DIR-}"; then
  echo "GRPC_INSTALL_DIR is set to <$GRPC_INSTALL_DIR>"
else
  echo "Be sure to set GRPC_INSTALL_DIR to the directory in which the"
  echo "C++ grpc package is installed. Then rerun this script."
  exit 1
fi

# For ejfat systems:
# export GRPC_INSTALL_DIR=/daqfs/gRPC/installation

# Want to pick up the correct protoc - the one installed with grpc
export PATH=$GRPC_INSTALL_DIR/bin:$PATH

# for Mac
#export DYLD_LIBRARY_PATH=$GRPC_INSTALL_DIR/lib:$DYLD_LIBRARY_PATH

# for Linux
if test -n "${LD_LIBRARY_PATH-}"; then
  setenv LD_LIBRARY_PATH $GRPC_INSTALL_DIR/lib:$LD_LIBRARY_PATH
else
  setenv LD_LIBRARY_PATH $GRPC_INSTALL_DIR/lib
endif

