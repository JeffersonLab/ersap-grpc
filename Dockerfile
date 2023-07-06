FROM debian:buster-slim AS builder
WORKDIR /usr/src/app

# Install necessary build tools
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update &&\
    apt-get install -y \
    build-essential \
    autoconf \
    git \
    pkg-config \
    automake \
    libtool \
    curl \
    make \
    g++ \
    unzip \
    libssl-dev \
    cmake \
    zlib1g-dev

# Build and install grpc
RUN git clone --recurse-submodules -b v1.56.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc.git
WORKDIR /usr/src/app/grpc
RUN mkdir -p cmake/build &&\
    cd cmake/build &&\
    cmake -DgRPC_INSTALL=ON \
          -DgRPC_BUILD_TESTS=OFF \
          -DCMAKE_INSTALL_PREFIX=/usr/local \
          ../.. &&\
    make -j8 &&\
    make install
ENV GRPC_INSTALL_DIR=/usr/local
COPY ./setupgrpc /usr/src/app/setupgrpc
RUN /bin/bash -c "source /usr/src/app/setupgrpc"

# Build ersap-grpc
WORKDIR /usr/src/app
COPY . /usr/src/app
RUN mkdir -p cmake/build
WORKDIR /usr/src/app/cmake/build
RUN cmake -DINSTALL_DIR=/usr/local ../.. && make

# Start from a new, smaller base image for runtime
FROM debian:buster-slim AS ersap-grpc
COPY --from=builder /usr/local /usr/local
COPY --from=builder /usr/src/app/cmake/build /app
WORKDIR /app
CMD [ "/bin/bash" ]
