# gRPC C++ ERSAP's EJFAT Backend

### Version

This is software that works with the original dynamic control plane.
It is now in a new "version-1" branch. It contains the update which
has the backend always reporting to the CP that it is ready
to accept data.

### Library

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

### Message Formats

The **loadbalancer.proto** file contains the protobuf definitions of the messages passed between control plane
and backend.

### Simulation Programs

#### cp_tester

The **cp_tester** program is a simulated backend used for control plane development.
It depends on the ejfat_grpc library.

#### cp_server

The **cp_server** program is a simulated control plane used together with cp_tester.
It depends on the ejfat_grpc library.

#### simSender

The **simSender** program sends data to the cp_tester. Embedded in each packet is:
1) the time in microseconds for the receiver to delay as simulated data processing
2) the total # of packets for the current event, and
3) the sequence of that packet.

It can also send sync data to the cp_server and depends on the ejfat_grpc library.


### Running a simulation

To help in setting things up, here is a suggestion that will allow data to flow slowly
enough and be processed even more slowly to allow the cp_tester's fifo to fill up over
the coarse of several seconds. Note that the LB is at addr = 172.19.22.244 and the host
is at addr = 172.19.22.15

1) **cp_server**

2) **cp_tester** -cp_addr 172.19.22.15  -a 172.19.22.15

3) **simSender** -mtu 9000 -d 5000 -host 172.19.22.244 -bufdelay -time 6500 -twidth 30 -sync -cphost 172.19.22.15 -bwidth 5000


Note that giving the -h arg will provide all program options. Notice also that the sender is
setup to vary both the delay and buffer sizes to get gaussian distributions for each.
The delay time between events sent is 5 millisec, but the processing time on the receiver is
6.5 millisec for each event, thus the fifo gradually fills on the tester.

### Extra Files

The **lb_cplane_async.cc/h** files are included as a place to start for asynchronous communication
between control plane and backend. They currently only use synchronous communication but could
be modified in the future.

### Setting up the Environment

The **setupgrpc** file helps sets up the environment for compilation, installation, and running.
Be sure to modify **GRPC_INSTALL_DIR** to point to the grpc installation directory.
It should contain grpc libs and includes.

### Compiling

The C++ distribution of grpc must be grabbed from github and built by hand, see:
https://github.com/grpc/grpc/blob/v1.55.0/BUILDING.md

Once grpc is compiled, install it in a directory that you specify in GRPC_INSTALL_DIR
There are couple things to know. Grpc's cmake config file will be in the
$GRPC_INSTALL_DIR/lib/cmake/grpc directory. This is what ersap-grpc's cmake will look for.
The other thing is to use the version of protoc that you get with the grpc installation.
Thus, put that in your path. This is what the script setupgrpc helps with.
Just edit it to set GRPC_INSTALL_DIR to the local installation of grpc,
and make any changes relevant to the operating system being used.

Do the following:

>**source setupgrpc  
mkdir -p cmake/build  
cd cmake/build  
cmake ../..  
make**  

For installation in some user-defined location, replace the last 2 lines with:

>**cmake ../.. -DINSTALL_DIR=my_installation_dir  
make install**  




