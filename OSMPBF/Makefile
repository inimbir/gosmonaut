all: build

init:
	go get -u -v github.com/gogo/protobuf/protoc-gen-gofast
	# github.com/gogo/protobuf/protoc-gen-gogo

build:
	# protoc --gofast_out=. *.proto
	protoc --gogo_out=. *.proto
