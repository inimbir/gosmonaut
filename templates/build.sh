#!/bin/bash
# go get github.com/cheekybits/genny
cat _binary_entity_map_template.go | genny gen "GenericEntityType=Node,Way" > ../binary_entity_map.go
