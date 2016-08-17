// Copyright 2016 Adam H. Leventhal. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/zabawaba99/firego"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

const SEARCHLIMIT = 100
const HNWINDOW = 14 * 24 * 60 * 60 // 2 weeks is the hacker news limit for editing

type datum struct {
	index float64
	value types.Value
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <dst>\n", path.Base(os.Args[0]))
	}

	var start int

	flag.IntVar(&start, "s", -1, "start time in seconds since the epoch")
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return
	}

	ds, err := spec.GetDataset(flag.Arg(0))
	if err != nil {
		fmt.Printf("Could not parse dst-dataset: %s\n", err)
		return
	}
	defer ds.Database().Close()

	mm := ds.HeadValue().(types.Map)

	if start == -1 {
		// Find the last entry that has a time field.
		key, value := mm.Last()

		var time types.Value
		var ok bool

		for i := 0; true; i += 1 {
			time, ok = value.(types.Struct).MaybeGet("time")
			if ok {
				break
			}

			if i >= SEARCHLIMIT {
				panic("no valid entry")
			}

			key = types.Number(float64(key.(types.Number)) - float64(i))
			value, ok = mm.MaybeGet(key)
			if !ok {
				panic("non-contigous shenanigans")
			}
		}

		start = int(time.(types.Number)) - HNWINDOW
	}

	fmt.Println(start)

	// Find the starting key.

	startKey := mapFindKeyBefore(mm, start)
	fmt.Println(types.EncodedIndexValue(startKey))

	// Process from startKey to maxItem to get caught up.

	update := firego.New("https://hacker-news.firebaseio.com/v0/updates", nil)
	newUpdate := make(chan firego.Event, 1000)
	if err := update.Watch(newUpdate); err != nil {
		panic(err)
	}

	max := firego.New("https://hacker-news.firebaseio.com/v0/maxitem", nil)
	var maxItem float64
	if err := max.Value(&maxItem); err != nil {
		panic(err)
	}

	newIndex := make(chan float64, 100)

	specialIndices := []float64{
		363, // Please tell us what features you'd like in news.ycombinator
	}

	go func() {
		for _, index := range specialIndices {
			newIndex <- index
		}

		for index := float64(startKey); index < maxItem; index += 1.0 {
		sent:
			for {
				select {
				case newIndex <- index:
					break sent
				case event := <-newUpdate:
					items := event.Data.(map[string]interface{})["items"].([]interface{})
					for _, item := range items {
						if item.(float64) > maxItem {
							maxItem = item.(float64)
						}
					}

					fmt.Println("got a new update ", int(maxItem))
				}
			}
		}
		close(newIndex)
	}()

	newDatum := make(chan datum, 100)

	workerPool(100, func() {
		churn(newIndex, newDatum)
	}, func() {
		close(newDatum)
	})

	for datum := range newDatum {
		mm = mm.Set(types.Number(datum.index), datum.value)
		fmt.Printf("%d/%d\n", int(datum.index), int(maxItem))
		//fmt.Println(types.EncodedIndexValue(datum.value))
	}

	fmt.Println("caught up")

	newMap := make(chan types.Map, 1) // This must be size 1

	go func() {
		mm := ds.HeadValue().(types.Map)
		for {
			mp := <-newMap
			if !mm.Equals(mp) {
				fmt.Println("committing")
				nds, err := ds.CommitValue(mp)
				if err != nil {
					panic(err)
				}
				ds = nds
				mm = mp
				fmt.Println("commit complete")
			} else {
				fmt.Println("no change")
			}
		}
	}()

	newMap <- mm

	// Enter the steady state now that we're caught up
	newIndex = make(chan float64, 1)
	newDatum = make(chan datum, 100)

	workerPool(1, func() {
		churn(newIndex, newDatum)
	}, func() {
		close(newDatum)
	})

	remaining := make(map[float64]bool)
	for event := range newUpdate {
		if event.Type == firego.EventTypeError {
			panic(event.Data)
		}

		items := event.Data.(map[string]interface{})["items"].([]interface{})
		for _, item := range items {
			newIndex <- item.(float64)
			remaining[item.(float64)] = true
		}

		fmt.Println("batch")
		for len(remaining) != 0 {
			datum := <-newDatum
			nmm := mm.Set(types.Number(datum.index), datum.value)
			if mm.Equals(nmm) {
				fmt.Println(int(datum.index), " no change")
			} else {
				fmt.Println(int(datum.index))
				mm = nmm
			}

			delete(remaining, datum.index)
		}

		// Poke our new map into the chan. If an old one is still in there, nudge it out and add our new one.
		select {
		case newMap <- mm:
		default:
			select {
			case _ = <-newMap:
			default:
			}
			newMap <- mm
		}
	}
}

func workerPool(count int, work func(), done func()) {
	workerDone := make(chan bool, 1)
	for i := 0; i < count; i += 1 {
		go func() {
			work()
			workerDone <- true
		}()
	}

	go func() {
		for i := 0; i < count; i += 1 {
			_ = <-workerDone
		}
		close(workerDone)
		done()
	}()
}

func mapFindFromKey(mm types.Map, value int) (types.Value, types.Value) {

	for i := 0; i < SEARCHLIMIT; i += 1 {
		midKey := types.Number(value - i)
		midVal, ok := mm.MaybeGet(midKey)
		if ok {
			_, ok = midVal.(types.Struct).MaybeGet("time")
			if ok {
				return midKey, midVal
			}
		}
	}

	panic("nothing found")
}

func mapFindKeyBefore(mm types.Map, time int) types.Number {
	minKey, _ := mm.First()
	maxKey, _ := mm.Last()

	for i := 0; i < SEARCHLIMIT; i += 1 {
		midIndex := int(minKey.(types.Number) + (maxKey.(types.Number)-minKey.(types.Number))/2)

		midKey, midVal := mapFindFromKey(mm, midIndex)

		if minKey == midKey {
			fmt.Println(types.EncodedIndexValue(midVal))
			return minKey.(types.Number)
		}

		midTime := midVal.(types.Struct).Get("time").(types.Number)

		if time < int(midTime) {
			maxKey = midKey
		} else {
			minKey = midKey
		}
	}

	panic("confusing")
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

func churn(newIndex <-chan float64, newData chan<- datum) {
	client := makeClient()

	for index := range newIndex {
		id := int(index)
		url := fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d", id)
		for attempts := 0; true; attempts += 1 {

			if attempts > 0 {
				// If we're having no luck after this much time, we'll declare this sucker the walking undead and try to get to it later.
				if attempts > 10 {
					fmt.Printf("Braaaaiiinnnssss %d\n", id)
					sendDatum(newData, "zombie", index, map[string]types.Value{
						"id":   types.Number(index),
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
				fmt.Printf("failed for %d (%d times) %s\n", id, attempts, err)
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
				fmt.Printf("success for %d after %d attempts\n", id, attempts)
			}

			sendDatum(newData, name.(string), index, data)
			break
		}
	}
}

func sendDatum(newData chan<- datum, name string, id float64, data map[string]types.Value) {
	st := types.NewStruct(name, data)
	d := datum{
		index: id,
		value: st,
	}

	select {
	case newData <- d:
	default:
		fmt.Println("blocked")
		newData <- d
	}
}
