# gRPC C++ EJFAT Load Balancer Backend

### library

The **ejfat_grpc** library is comprised of **lb_cplane.cc** and **lb_cplane.h** .
These employ a synchronous communication between the backend and the conntrol plane
in an ejfat application.

The **BackEnd** class is for storing data.

The **LoadBalancerServiceImpl** class acts as a simulated control plane. It is setup to do
synchronous communication with the backend. It defines commands that handle the backend's
call to invoke an action in the server such as: Register, SendState, and Deregister.
It also defines the runServer method which implements these functions in a grpc server.

The **LbControlPlaneClient** class is used by a backend in order to communicate with a
simulated (or perhaps a real) control plane server. It allows the backend to
Register, SendState, and Deregister as well as control the state that it sends.

### simulated backend

The **cp_tester** program is a simulated backend used for control plane development.
It depends on the ejfat_grpc library.

### message formats

The **loadbalancer.proto** file contains the protobuf definitions of the messages passed between control plane
and backend.

### setting up the environment

The **setupgrpc** file sets up the environment for compilation, installation, and running.
Be sure to modify **GRPC_INSTALL_DIR** to point to the grpc installation directory.
It should contain grpc libs and includes.

### extra files

The **lb_cplane_async.cc/h** files are included as a place to start for asynchronous communication
between control plane and backend. They currently only use synchronous communication but could
be modified in the future.

### compiling

To set things up, modify the setupgrpc bash file to set GRPC_INSTALL_DIR to the local installation
of necessary grpc libs and includes. Do the following:

>**setupgrpc  
mkdir -p cmake/build  
cd cmake/build  
cmake ../..  
make**  

For installation in some user-defined location, replace the last 2 lines with:

>**cmake ../.. -DINSTALL_DIR=my_installation_dir  
make install**  




