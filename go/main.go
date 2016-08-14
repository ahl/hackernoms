// Copyright 2016 Adam H. Leventhal. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"math"
	"net"
	"net/http"
	//"os"
	//"os/signal"
	"reflect"
	"sync"
	"time"

	"github.com/zabawaba99/firego"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

type ItemIterator struct {
	lock       *sync.Mutex
	cv         *sync.Cond
	nextItem   uint32
	maxItem    uint32
	extraItems []uint32
	max        *firego.Firebase
	update     *firego.Firebase
}

func NewItemIterator() *ItemIterator {
	m := &sync.Mutex{}
	ii := &ItemIterator{m, sync.NewCond(m), 0, 0, nil, nil, nil}

	ii.max = firego.New("https://hacker-news.firebaseio.com/v0/maxitem", nil)
	var val float64
	if err := ii.max.Value(&val); err != nil {
		panic(err)
	}

	ii.maxItem = uint32(val)
	ii.nextItem = 1
	ii.nextItem = ii.maxItem - 4000

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

func (ii *ItemIterator) addExtra(item uint32) {
	ii.lock.Lock()
	defer ii.lock.Unlock()
	if item < ii.nextItem {
		ii.extraItems = append(ii.extraItems, item)
		ii.cv.Broadcast()
	}
}

// Return the next element; block if none is available.
func (ii *ItemIterator) Next() (uint32, bool) {
	ii.lock.Lock()
	defer ii.lock.Unlock()

	for {
		if ll := len(ii.extraItems); ll != 0 {
			item := ii.extraItems[ll-1]
			ii.extraItems = ii.extraItems[:ll-1]
			return item, true
		}

		if ii.nextItem <= ii.maxItem {
			item := ii.nextItem
			ii.nextItem += 1
			return item, false
		}

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
	extra bool
}

func main() {
	fmt.Println("starting")

	ii := NewItemIterator()

	ds, err := spec.GetDataset("http://localhost:8000::hn")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ds.Database().Close()

	depth := 150
	newData := make(chan datum, depth)
	streamData := make(chan types.Value, depth)

	start := time.Now()

	newMap := types.NewStreamingMap(ds.Database(), streamData)

	// Grab the data from hacker news.
	for i := 0; i < 500; i++ {
		go churn(newData, ii)
	}

	var count uint32
	for {
		d := <-newData
		if !d.extra {
			count += 1
		}

		// XXX move status reporting out of here to a different go routine
		total := ii.Max()
		if count%1000 == 0 {
			dur := time.Since(start)
			eta := time.Duration(float64(dur) * float64(total-count) / float64(count))

			fmt.Printf("sent:  %d/%d  time remaining: %s\n", d.id, total, eta)
			fmt.Println(d.id, " <=> ", total-uint32(depth)*2)
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

	// XXX make sure we're converging; look at the length of the extras array.
	// XXX better status reporting since this is the persistent mode of operation.
	// XXX figure out what I want to do if we come up and there's data already loaded.

	for {
		d := <-newData

		newCount := 0
		extraCount := 0
		if d.extra {
			extraCount += 1
		} else {
			newCount += 1
		}

		mm = mm.Set(d.key, d.value)

		last := time.Now()
		blocked := false
		for !blocked && time.Since(last) < 5*time.Second {
			select {
			case d = <-newData:
				if d.extra {
					extraCount += 1
				} else {
					newCount += 1
				}
				mm = mm.Set(d.key, d.value)
			default:
				blocked = true
			}
		}

		fmt.Printf("new: %d extra: %d   extras: %d\n", newCount, extraCount, len(ii.extraItems))

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

func churn(newData chan<- datum, ii *ItemIterator) {
	var tr *http.Transport
	tr = &http.Transport{
		//DisableKeepAlives: true, // https://code.google.com/p/go/issues/detail?id=3514
		Dial: func(network, address string) (net.Conn, error) {
			start := time.Now()
			c, err := net.DialTimeout(network, address, firego.TimeoutDuration)
			tr.ResponseHeaderTimeout = firego.TimeoutDuration - time.Since(start)
			return c, err
		},
	}

	client := &http.Client{
		Transport: tr,
		//CheckRedirect: redirectPreserveHeaders,
	}

	for {
		id, extra := ii.Next()
		url := fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d", id)
		for {
			fb := firego.New(url, client)

			var val map[string]interface{}
			err := fb.Value(&val)
			if err != nil {
				continue
				//panic(err)
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
			if ok {
				st := types.NewStruct(name.(string), data)
				d := datum{
					id:    id,
					key:   data["id"].(types.Number),
					value: st,
					extra: extra,
				}

				select {
				case newData <- d:
				default:
					fmt.Println("blocked")
					newData <- d
				}
				break
			}
		}
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
