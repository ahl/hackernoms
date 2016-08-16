// Copyright 2016 Adam H. Leventhal. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/zabawaba99/firego"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

type itemKind int

const (
	NORMAL itemKind = iota
	EXTRA
	ZOMBIE
)

type ItemIterator struct {
	lock        *sync.Mutex
	cv          *sync.Cond
	nextItem    uint32
	maxItem     uint32
	extraItems  []uint32
	zombieItems []uint32
	max         *firego.Firebase
	update      *firego.Firebase
}

func NewItemIterator() *ItemIterator {
	m := &sync.Mutex{}
	ii := &ItemIterator{m, sync.NewCond(m), 0, 0, nil, nil, nil, nil}

	ii.max = firego.New("https://hacker-news.firebaseio.com/v0/maxitem", nil)
	var val float64
	if err := ii.max.Value(&val); err != nil {
		panic(err)
	}

	ii.maxItem = uint32(val)
	ii.nextItem = 1
	//ii.nextItem = ii.maxItem - 100000
	//ii.nextItem = 9748489 - 10000

	maxNotify := make(chan firego.Event)
	if err := ii.max.Watch(maxNotify); err != nil {
		panic(err)
	}

	go func() {
		for event := range maxNotify {
			ii.setMax(uint32(event.Data.(float64)))
		}
	}()

	ii.update = firego.New("https://hacker-news.firebaseio.com/v0/updates", nil)
	updateNotify := make(chan firego.Event)
	if err := ii.update.Watch(updateNotify); err != nil {
		panic(err)
	}

	go func() {
		for event := range updateNotify {
			for _, item := range event.Data.(map[string]interface{})["items"].([]interface{}) {
				ii.addExtra(uint32(item.(float64)))
			}
		}
	}()

	return ii
}

func (ii *ItemIterator) setMax(max uint32) {
	ii.lock.Lock()
	defer ii.lock.Unlock()
	ii.maxItem = max
	ii.cv.Broadcast()
}

func (ii *ItemIterator) addExtra(id uint32) {
	ii.lock.Lock()
	defer ii.lock.Unlock()
	if id < ii.nextItem {
		ii.extraItems = append(ii.extraItems, id)
		ii.cv.Broadcast()
	}
}

func (ii *ItemIterator) addZombie(id uint32) {
	ii.lock.Lock()
	defer ii.lock.Unlock()
	ii.zombieItems = append(ii.zombieItems, id)
	ii.cv.Broadcast()
}

// Return the next element; block if none is available.
func (ii *ItemIterator) Next() (uint32, itemKind) {
	ii.lock.Lock()
	defer ii.lock.Unlock()

	for {
		if ll := len(ii.extraItems); ll != 0 {
			id := ii.extraItems[ll-1]
			ii.extraItems = ii.extraItems[:ll-1]
			return id, EXTRA
		}

		if ii.nextItem <= ii.maxItem {
			id := ii.nextItem
			ii.nextItem += 1
			return id, NORMAL
		}

		/*
			if ll := len(ii.zombieItems); ll != 0 {
				id := ii.zombieItems[ll-1]
				ii.zombieItems = ii.zombieItems[:ll-1]
				return id, ZOMBIE
			}
		*/

		ii.cv.Wait()
	}
}

func (ii *ItemIterator) Max() uint32 {
	return ii.maxItem
}

type datum struct {
	id    uint32
	key   types.Number
	value types.Struct
	kind  itemKind
}

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <dest-dataset-spec>\n", os.Args[0])
	}
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Println("Required dest-dataset param not provided")
		return
	}

	ds, err := spec.GetDataset(flag.Arg(0))
	if err != nil {
		fmt.Printf("Could not parse dest-dataset: %s\n", err)
		return
	}
	defer ds.Database().Close()

	fmt.Println("starting")

	ii := NewItemIterator()

	depth := 150
	newData := make(chan datum, depth)
	streamData := make(chan types.Value, depth)

	start := time.Now()

	newMap := types.NewStreamingMap(ds.Database(), streamData)

	// Grab the data from hacker news.
	for i := 0; i < 500; i++ {
		go churn(newData, ii, i)
	}

	var count uint32
	counts := make([]uint32, 3)
	for {
		d := <-newData

		count += 1
		counts[d.kind] += 1

		// XXX move status reporting out of here to a different go routine
		total := ii.Max()
		if count%1000 == 0 {
			dur := time.Since(start)
			eta := time.Duration(float64(dur) * float64(total-count) / float64(count))

			fmt.Printf("sent: %d/%d (%d)  x: %d z: %d  eta: %s\n", counts[NORMAL], total, d.id, counts[EXTRA], counts[ZOMBIE], eta)
		}

		streamData <- d.key
		streamData <- d.value

		// Too close for missiles; switching to guns. Close down the stream which will trigger the initial creation of the Map; exit the loop to wait for it to show up. Even though we try to drain newData, there may still be data left in newData, but we can pick that up later.
		//if count > total-uint32(depth)*2 {
		if d.id > total-uint32(depth)*2 {
			// if count > 10000 {
			done := false
			for !done {
				select {
				case d := <-newData:
					streamData <- d.key
					streamData <- d.value
				default:
					done = true
				}
			}
			close(streamData)
			break
		}
	}

	fmt.Println("generating map...")

	// Wait for the map to build; this could a little bit oftime so we need to figure out a plan for buffering data in the meantime. The most critical thing to stay on top of is going to be "extra" items in the iterator.
	mm := <-newMap

	getMap := make(chan types.Map)

	go func() {
		fmt.Println("first commit...")
		mp := mm
		for {
			nds, err := ds.CommitValue(mp)
			if err != nil {
				panic(err)
			}
			ds = nds
			mp = <-getMap
		}
	}()

	fmt.Println("processing...")

	// XXX better status reporting since this is the persistent mode of operation.
	// XXX figure out what I want to do if we come up and there's data already loaded.

	for {
		d := <-newData

		counts := make([]uint32, 3)
		counts[d.kind] += 1

		mm = mm.Set(d.key, d.value)

		last := time.Now()
		blocked := false
		for !blocked && time.Since(last) < 5*time.Second {
			select {
			case d = <-newData:
				counts[d.kind] += 1
				mm = mm.Set(d.key, d.value)
			default:
				blocked = true
			}
		}

		fmt.Printf("n/x/z: %d/%d/%d  queued x/z: %d/%d", counts[NORMAL], counts[EXTRA], counts[ZOMBIE], len(ii.extraItems), len(ii.zombieItems))

		// Poke our new map into the chan. If an old one is still in there, nudge it out and add our new one.
		select {
		case getMap <- mm:
		default:
			select {
			case _ = <-getMap:
			default:
			}
			getMap <- mm
		}
	}
}

func makeClient() *http.Client {
	var tr *http.Transport
	tr = &http.Transport{
		Dial: func(network, address string) (net.Conn, error) {
			return net.DialTimeout(network, address, 30*time.Second)
		},
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   time.Second * 30,
	}

	return client
}

func churn(newData chan<- datum, ii *ItemIterator, me int) {
	client := makeClient()

	for {
		id, kind := ii.Next()
		url := fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d", id)
		for attempts := 0; true; attempts += 1 {

			if attempts > 2 {
				// If we're having no luck after this much time, we'll declare this sucker the walking undead and try to get to it later.
				if attempts > 10 {
					fmt.Printf("Braaaaiiinnnssss %d\n", id)
					ii.addZombie(id)
					sendDatum(newData, "zombie", id, ZOMBIE, map[string]types.Value{
						"id":   types.Number(id),
						"type": types.String("zombie"),
					})
					break
				}
				if attempts == 5 {
					client = makeClient()
				}
				time.Sleep(time.Millisecond * 100 * time.Duration(attempts))
			}

			fb := firego.New(url, client)

			var val map[string]interface{}
			err := fb.Value(&val)
			if err != nil {
				fmt.Printf("(%d) failed for %d (%d times) %s\n", me, id, attempts, err)
				continue
			}

			data := make(map[string]types.Value)
			for k, v := range val {
				switch vv := v.(type) {
				case string:
					data[k] = types.String(vv)
				case float64:
					data[k] = types.Number(vv)
				case bool:
					data[k] = types.Bool(vv)
				case []interface{}:
					ll := types.NewList()
					for _, elem := range vv {
						ll = ll.Append(types.Number(elem.(float64)))
					}
					data[k] = ll
				default:
					panic(reflect.TypeOf(v))
				}
			}

			name, ok := val["type"]
			if !ok {
				fmt.Printf("no type for id %d; trying again\n", id)
				continue
			}

			if attempts > 1 {
				fmt.Printf("(%d) success for %d after %d attempts\n", me, id, attempts)
			}

			sendDatum(newData, name.(string), id, kind, data)
			break
		}
	}
}

func sendDatum(newData chan<- datum, name string, id uint32, kind itemKind, data map[string]types.Value) {
	st := types.NewStruct(name, data)
	d := datum{
		id:    id,
		key:   data["id"].(types.Number),
		value: st,
		kind:  kind,
	}

	select {
	case newData <- d:
	default:
		fmt.Println("blocked")
		newData <- d
	}
}

func Measure(subject func()) time.Duration {
	start := time.Now()
	subject()
	return time.Since(start)
}

type Distribution struct {
	counts [64]uint64
	sums   [64]uint64
	count  uint64
	sum    uint64
	sum2   uint64
}

func NewDistribution() *Distribution {
	return &Distribution{}
}

func (dist *Distribution) Add(value uint64) {
	dist.sum += value
	dist.sum2 += value * value
	dist.count += 1

	var hibit uint
	for hibit = 64; hibit > 0; hibit -= 1 {
		if value&(1<<(hibit-1)) != 0 {
			break
		}
	}

	dist.counts[hibit] += 1
	dist.sums[hibit] += value
}

func (dist *Distribution) Avg() float64 {
	return float64(dist.sum) / float64(dist.count)
}

func (dist *Distribution) StdDev() float64 {
	avg := float64(dist.sum) / float64(dist.count)
	avg2 := float64(dist.sum2) / float64(dist.count)

	return math.Sqrt(avg2 - avg*avg)
}

func (dist *Distribution) Hist() {
	var min, max uint

	for max = 63; max > 0; max -= 1 {
		if dist.counts[max] != 0 {
			break
		}
	}

	for min = 0; min < max; min += 1 {
		if dist.counts[min] != 0 {
			break
		}
	}

	for i := min; i <= max; i++ {
		fmt.Printf("%8d %d\n", 1<<i, dist.counts[i])
	}
}
