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

	// Grab the root struct; convert from the old format if needed.
	var head types.Struct
	hv := ds.HeadValue()

	switch h := hv.(type) {
	case types.Map:
		head = types.NewStruct("HackerNoms", types.StructData{
			"items": h,
			"top":   types.NewList(types.Number(0)),
		})
	case types.Struct:
		head = h
	}

	mm := head.Get("items").(types.Map)

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

	// Find the starting key.
	startKey, startVal := mapFindKeyBefore(mm, start)
	tt := startVal.(types.Struct).Get("time").(types.Number)
	fmt.Println(types.EncodedIndexValue(startVal))
	fmt.Printf("posted %s ago\n", time.Since(time.Unix(int64(tt), 0)))

	// Process from startKey to maxItem to get caught up. We update maxItem based on items that we see in updates.
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

	// These items may update beyond the standard two week window.
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
		nmm := mm.Set(types.Number(datum.index), datum.value)
		if mm.Equals(nmm) {
			fmt.Printf("%d/%d no change\n", int(datum.index), int(maxItem))
		} else {
			mm = nmm
			fmt.Printf("%d/%d\n", int(datum.index), int(maxItem))
		}
	}

	fmt.Println("caught up")

	newHead := make(chan types.Struct, 1) // This must be size 1

	go func() {
		oldHead := ds.HeadValue()
		for {
			head := <-newHead
			if !head.Equals(oldHead) {
				fmt.Println("committing")
				nds, err := ds.CommitValue(head)
				if err != nil {
					panic(err)
				}
				ds = nds
				oldHead = head
				fmt.Println("commit complete")
			} else {
				fmt.Println("no change")
			}
		}
	}()

	head = head.Set("items", mm)
	newHead <- head

	// Enter the steady state now that we're caught up
	newIndex = make(chan float64, 1)
	newDatum = make(chan datum, 100)

	workerPool(1, func() {
		churn(newIndex, newDatum)
	}, func() {
		close(newDatum)
	})

	// Also start watching topstories.
	newTop := make(chan firego.Event)
	top := firego.New("https://hacker-news.firebaseio.com/v0/topstories", nil)
	if err := top.Watch(newTop); err != nil {
		panic(err)
	}

	remaining := make(map[float64]bool)
	topItems := make([]types.Value, 0, 500)
	for {
		oldHead := head
		select {
		case event := <-newUpdate:
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
			head = head.Set("items", mm)

		case event := <-newTop:
			if event.Type == firego.EventTypeError {
				panic(event.Data)
			}

			items := event.Data.([]interface{})
			topItems = topItems[:len(items)]
			for i, item := range items {
				topItems[i] = types.Number(item.(float64))
			}

			head = head.Set("top", types.NewList(topItems...))
		}

		if !oldHead.Equals(head) {
			// Poke our new head into the chan. If an old one is still in there, nudge it out and add our new one.
			select {
			case newHead <- head:
			default:
				select {
				case _ = <-newHead:
				default:
				}
				newHead <- head
			}
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

func mapFindKeyBefore(mm types.Map, time int) (types.Number, types.Value) {
	minKey, _ := mm.First()
	maxKey, _ := mm.Last()

	for i := 0; i < SEARCHLIMIT; i += 1 {
		midIndex := int(minKey.(types.Number) + (maxKey.(types.Number)-minKey.(types.Number))/2)

		midKey, midVal := mapFindFromKey(mm, midIndex)

		if minKey == midKey {
			return midKey.(types.Number), midVal
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
