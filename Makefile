# init project path
HOMEDIR := $(shell pwd)
OUTDIR  := $(HOMEDIR)/output

# init command params
GO      := go
GOPATH  := $(shell $(GO) env GOPATH)
GOMOD   := $(GO) mod
GOBUILD := $(GO) build
GOTEST  := $(GO) test -gcflags="-N -l"
GOPKGS  := $$($(GO) list ./...| grep -vE "vendor" | grep -vE "github.com/PaddlePaddle/PaddleFlow/pkg/fs/fuse/ufs")
GOARCH := $(shell $(GO) env GOARCH)
GOOS := $(shell $(GO) env GOOS)
export PATH := $(GOPATH)/bin/:$(PATH)

CC  := $(shell $(GO) env CC)
CC_FOR_TARGET := $(shell $(GO) env CC_FOR_TARGET)
# test cover files
COVPROF := $(HOMEDIR)/covprof.out  # coverage profile
COVFUNC := $(HOMEDIR)/covfunc.txt  # coverage profile information for each function
COVHTML := $(HOMEDIR)/covhtml.html # HTML representation of coverage profile

GIT_COMMIT  = `git rev-parse HEAD`
GIT_DATE    = `date "+%Y-%m-%d %H:%M:%S"`
GIT_VERSION = `git --version`
GIT_BRANCH  = `git rev-parse --abbrev-ref HEAD`

LD_FLAGS    = " \
    -X 'github.com/PaddlePaddle/PaddleFlow/pkg/version.GitVersion=${GIT_VERSION}' \
    -X 'github.com/PaddlePaddle/PaddleFlow/pkg/version.GitCommit=${GIT_COMMIT}' \
    -X 'github.com/PaddlePaddle/PaddleFlow/pkg/version.GitBranch=${GIT_BRANCH}' \
    -X 'github.com/PaddlePaddle/PaddleFlow/pkg/version.BuildDate=${GIT_DATE}' \
    '-extldflags=-static' \
    -w -s"

# make, make all
all: prepare compile package

# make prepare, download dependencies
prepare: gomod arch

gomod:
	$(GO) env -w GO111MODULE=on
	#$(GO) env -w GOPROXY=https://goproxy.io,direct
	$(GO) env -w CGO_ENABLED=0
	$(GOMOD) download

arch:
    ifeq ($(GOARCH),amd64)
		@echo "arch是$(GOARCH)"
    else
		@echo "arch是$(GOARCH)"
        CC=aarch64-linux-gnu-gcc
        CC_FOR_TARGET=gcc-aarch64-linux-gnu
    endif


# make compile
compile: build

build:
	CGO_ENABLED=1 CC=$(CC) CC_FOR_TARGET=$(CC_FOR_TARGET) $(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/paddleflow $(HOMEDIR)/cmd/server/main.go
	$(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/pfs-fuse     $(HOMEDIR)/cmd/fs/fuse/main.go
	$(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/csi-plugin   $(HOMEDIR)/cmd/fs/csi-plugin/main.go
	$(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/cache-worker $(HOMEDIR)/cmd/fs/location-awareness/cache-worker/main.go


build-macos:
	CGO_ENABLED=1 CC=gcc GOOS= GOARCH=amd64 $(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/paddleflow $(HOMEDIR)/cmd/server/main.go
	$(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/pfs-fuse     $(HOMEDIR)/cmd/fs/fuse/main.go
	$(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/csi-plugin   $(HOMEDIR)/cmd/fs/csi-plugin/main.go
	$(GOBUILD) -ldflags ${LD_FLAGS} -trimpath -o $(HOMEDIR)/cache-worker $(HOMEDIR)/cmd/fs/location-awareness/cache-worker/main.go


build-by-xgo:
	$(GO) install src.techknowlogick.com/xgo@latest
	$(GOPATH)/bin/xgo -ldflags ${LD_FLAGS} -trimpath -out $(HOMEDIR)/paddleflow -targets $(GOOS)/$(GOARCH) -pkg $(HOMEDIR)/cmd/server/main.go .

# make doc
doc:
	$(GO) get -u github.com/swaggo/swag/cmd/swag@v1.7.6
	swag init -g router.go --parseDependency --parseInternal  -o $(HOMEDIR)/docs/api -d $(HOMEDIR)/pkg/apiserver/router/v1/
	mkdir -p $(OUTDIR)/docs
	mv $(HOMEDIR)/docs/api $(OUTDIR)/docs

# make test, test your code
test: prepare mock-gen test-case
mock-gen:
	$(GO) get golang.org/x/tools/go/packages
	$(GO) get github.com/golang/mock/mockgen@v1.4.4
	mockgen -destination=pkg/pipeline/mock_job.go -source=pkg/pipeline/job.go -package=pipeline
test-case:
	$(GOTEST) -v -cover $(GOPKGS)

# make package
package:
	mkdir -p $(OUTDIR)/bin
	mv $(HOMEDIR)/paddleflow   $(OUTDIR)/bin
	mv $(HOMEDIR)/pfs-fuse     $(OUTDIR)/bin
	mv $(HOMEDIR)/csi-plugin   $(OUTDIR)/bin
	mv $(HOMEDIR)/cache-worker $(OUTDIR)/bin
	mv $(HOMEDIR)/pkg/fs/utils/mount.sh $(OUTDIR)/bin

# make clean
clean:
	$(GO) clean
	rm -rf $(OUTDIR)

# avoid filename conflict and speed up build
.PHONY: all prepare compile test package clean build
