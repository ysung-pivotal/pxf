# `pxf cluster` CLI

## Getting Started

1. Ensure you are set up for PXF development by following the README.md at the root of this repository. This tool requires Go version 1.9 or higher. Follow the directions [here](https://golang.org/doc/) to get the language set up.

1. Go to the pxf-cluster folder and install dependencies
   ```
   cd pxf/server/pxf-cli/go/src/pxf-cli
   go get github.com/golang/dep/cmd/dep
   go get github.com/onsi/ginkgo/ginkgo
   make depend
   ```

1. Run the tests
   ```
   make test
   ```

1. Build the CLI
   ```
   make
   ```
   This will put the binary at `pxf/server/pxf-cli/go/bin/pxf-cli`. You can also install the binary into `${PXF_HOME}/bin/pxf-cli` with:
   ```
   make install
   ```

## Adding New Dependencies

1. Import the dependency in some source file (otherwise `dep` will refuse to install it)

2. Add the dependency to Gopkg.toml

3. Run `make depend`.
