export GORACE := halt_on_error=1

all: cover

cover:
	go install -v
	go test -v -coverprofile=profile.cov

test: check
	go test -v

race: check
	go test -v -race

check:
	gofmt -l -s -w .
	go vet
	golint

# Compile templates
generate: binary_entity_map.go

binary_entity_map.go:
	cat templates/_binary_entity_map_template.go | genny gen "GenericEntityType=Node,Way" > binary_entity_map.go
