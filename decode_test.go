package osmpbf_test

import (
	"fmt"
	"github.com/qedus/osmpbf"
	"io"
	"os"
	"testing"
	"time"
)

func TestDecoder(t *testing.T) {
	//f, err := os.Open("planet-latest.osm.pbf")
	f, err := os.Open("greater_london.osm.pbf")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer f.Close()

	d := osmpbf.NewDecoder(f)
	n, w, r := 0, 0, 0
	count, start := 0, time.Now()
	now := start
	for {
		if v, err := d.Decode(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err.Error())
		} else {
			switch v := v.(type) {
			case *osmpbf.Node:
				n++
			case *osmpbf.Way:
				w++
			case *osmpbf.Relation:
				r++
			default:
				t.Fatalf("unknwon type %T", v)
			}
		}
		count++
		if count%1000000 == 0 {
			newNow := time.Now()
			dur := newNow.Sub(now)
			fmt.Printf("%s\t%s\t%d\t%d\t%d\t%d\n",
				newNow.Sub(start), dur, count, n, w, r)
			now = newNow
		}
	}
	fmt.Printf("Nodes: %d, Ways: %d, Relations: %d\n", n, w, r)
}
