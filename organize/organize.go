// Copyright 2016 Adam H. Leventhal. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

// Turn the items into threads:
// Map<Number, Struct Story {
//	id Number
//	time Number
//
//	// Optional
//	deleted, dead Bool
//	descendants, score Number
//	text, url, title, by String
//
//	comments List<Struct Comment {
//		id Number
//		time Number
//
//		// Optional
//		deleted, dead Bool
//		text, by String
//
//		comments List<Cycle<0>>
//	}>
// }>
//
// Turn the top stories into a list
// List<Struct StorySummary {
//	id Number
//	title String
//	url String | Nothing
//	score Number
//	by String
//	time Number
//	descendants Number
//
//	story Ref<Struct Story { ... }>
// }>

var nothing types.Value
var nothingType *types.Type

func init() {
	nothing = types.NewStruct("Nothing", types.StructData{})
	nothingType = nothing.Type()
}

var commentType *StructType
var storyType *StructType

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <src> <dst>\n", path.Base(os.Args[0]))
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		return
	}

	// Just make sure that the source is valid
	srcdb, _, err := spec.GetDataset(os.Args[1])
	if err != nil {
		fmt.Printf("invalid source dataset: %s\n", os.Args[1])
		fmt.Printf("%s\n", err)
		return
	}
	srcdb.Close()

	db, ds, err := spec.GetDataset(os.Args[2])
	if err != nil {
		fmt.Printf("invalid destination dataset: %s\n", os.Args[2])
		fmt.Printf("%s\n", err)
		return
	}
	defer db.Close()

	// Create our types.
	optionString := types.MakeUnionType(types.StringType, nothingType)
	optionNumber := types.MakeUnionType(types.NumberType, nothingType)
	optionBool := types.MakeUnionType(types.BoolType, nothingType)

	commentType = MakeStructType("Comment", []FieldType{
		{"id", types.NumberType},
		{"time", types.NumberType},

		{"text", optionString},
		{"by", optionString},

		{"deleted", optionBool},
		{"dead", optionBool},

		{"comments", types.MakeListType(types.MakeCycleType(0))},
	})

	storyType = MakeStructType("Story", []FieldType{
		{"id", types.NumberType},
		{"time", types.NumberType},

		{"title", optionString},
		{"url", optionString},
		{"text", optionString},
		{"by", optionString},

		{"deleted", optionBool},
		{"dead", optionBool},

		{"descendants", optionNumber},
		{"score", optionNumber},

		{"comments", types.MakeListType(commentType.t)},
	})

	hv, ok := ds.MaybeHeadValue()
	if !ok {
		fmt.Println("doing the initial sync...")
		ds = bigSync(ds)
		hv = ds.HeadValue()
	}

	dstHead := hv.(types.Struct)

	for {
		srcdb, srcds, err := spec.GetDataset(os.Args[1])
		if err != nil {
			panic(err)
		}

		dstHead = ds.HeadValue().(types.Struct)

		oldHeadHash := hash.Parse(string(dstHead.Get("head").(types.String)))
		oldHead := srcdb.ReadValue(oldHeadHash).(types.Struct).Get("value").(types.Struct)
		currentHead := srcds.HeadValue().(types.Struct)

		if oldHead.Equals(currentHead) {
			// This is designed to run in a loop, but either Noms or Go is profoundly sloppy with memory. Put the loop outside the scope of this program (i.e. in the shell or an AWS ECS task) so that we get a fresh address space each time.
			fmt.Println("no changes; exiting")
			break
		} else {
			dstHead = update(ds, oldHead, currentHead, dstHead)
			dstHead = dstHead.Set("head", types.String(srcds.Head().Hash().String()))

			ds, err = db.CommitValue(ds, dstHead)
			if err != nil {
				panic(err)
			}
		}

		srcdb.Close()
	}
}

func bigSync(ds datas.Dataset) datas.Dataset {
	srcdb, srcds, err := spec.GetDataset(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer srcdb.Close()

	head := srcds.HeadValue().(types.Struct)
	allItems := head.Get("items").(types.Map)
	topStories := head.Get("top").(types.List)

	newItem := make(chan types.Struct, 100)
	newStory := make(chan types.Value, 100)

	lastKey, _ := allItems.Last()
	lastIndex := int(lastKey.(types.Number))

	go func() {
		//topStories.Iter(func(index types.Value, _ uint64) bool {
		allItems.Iter(func(id, value types.Value) bool {
			//value := allItems.Get(index)
			item := value.(types.Struct)

			// Note that we're explicitly excluding items of type "job" and "poll" which may also be found in the list of top items.
			switch item.Type().Desc.(types.StructDesc).Name {
			case "story":
				newItem <- item
			}

			return false
		})
		close(newItem)
	}()

	workerPool(50, makeStories(allItems, newItem, newStory), func() {
		close(newStory)
	})

	streamData := make(chan types.Value, 100)
	newMap := types.NewStreamingMap(ds.Database(), streamData)

	start := time.Now()
	count := 0

	for story := range newStory {
		id := story.(types.Struct).Get("id")

		count++
		if count%1000 == 0 {
			n := int(id.(types.Number))
			dur := time.Since(start)
			eta := time.Duration(float64(dur) * float64(lastIndex-n) / float64(n))
			fmt.Printf("%d/%d %s\n", n, lastIndex, eta)
		}

		streamData <- id
		streamData <- story
	}
	close(streamData)

	fmt.Println("stream completed")

	stories := <-newMap

	fmt.Println("map created")

	top := topList(ds, topStories, stories)

	srcds, err = srcdb.CommitValue(srcds, types.NewStruct("HackerNoms", types.StructData{
		"stories": stories,
		"top":     top,
		"head":    types.String(srcds.Head().Hash().String()),
	}))
	if err != nil {
		panic(err)
	}

	return ds
}

func makeStories(allItems types.Map, newItem <-chan types.Struct, newStory chan<- types.Value) func() {
	return func() {
		for item := range newItem {
			id := item.Get("id")
			fmt.Printf("working on story %d\n", int(id.(types.Number)))

			// Known stubs with just id and type
			if item.Type().Desc.(types.StructDesc).Len() == 2 {
				item.Get("type") // or panic
				continue
			}

			newStory <- NewStructWithType(storyType, types.ValueSlice{
				id,
				item.Get("time"),
				OptionGet(item, "title"),
				OptionGet(item, "url"),
				OptionGet(item, "text"),
				OptionGet(item, "by"),
				OptionGet(item, "deleted"),
				OptionGet(item, "dead"),
				OptionGet(item, "descendants"),
				OptionGet(item, "score"),
				comments(item, allItems),
			})
		}
	}
}

func OptionGet(st types.Struct, field string) types.Value {
	value, ok := st.MaybeGet(field)
	if ok {
		return value
	} else {
		return nothing
	}
}

func SomeOf(v types.Value) types.Value {
	if v.Type() == nothingType {
		panic("nothing!")
	}
	return v
}

func SomeOr(v types.Value, def types.Value) types.Value {
	if v.Type() == nothingType {
		return def
	}
	return v
}

// Process children; |item| may be a story or a comment.
func comments(item types.Value, allItems types.Map) types.Value {
	ret := types.NewList()

	c, ok := item.(types.Struct).MaybeGet("kids")
	if ok {
		c.(types.List).IterAll(func(id types.Value, _ uint64) {
			value, ok := allItems.MaybeGet(id)
			if !ok {
				fmt.Printf("unable to look up %d from %d\n", int(id.(types.Number)), int(item.(types.Struct).Get("id").(types.Number)))
				//panic(fmt.Sprintf("unable to look up %d from %d", int(id.(types.Number)), int(item.(types.Struct).Get("id").(types.Number))))
				return
			}

			subitem := value.(types.Struct)

			// Ignore stubs and zombies
			_, ok = subitem.MaybeGet("time")
			if !ok {
				return
			}

			comm := NewStructWithType(commentType, types.ValueSlice{
				id,
				subitem.Get("time"),
				OptionGet(subitem, "text"),
				OptionGet(subitem, "by"),
				OptionGet(subitem, "deleted"),
				OptionGet(subitem, "dead"),
				comments(subitem, allItems),
			})
			ret = ret.Append(comm)
		})
	}

	return ret
}

func topList(ds datas.Dataset, srcTop types.List, dstStories types.Map) types.List {
	streamData := make(chan types.Value, 10)
	newList := types.NewStreamingList(ds.Database(), streamData)

	srcTop.IterAll(func(item types.Value, _ uint64) {
		id := item.(types.Number)
		v, ok := dstStories.MaybeGet(id)
		if !ok {
			fmt.Printf("%d in top stories, but not in map\n", int(id))
			return
		}

		story := v.(types.Struct)

		streamData <- types.NewStruct("StorySummary", types.StructData{
			"id":          id,
			"title":       SomeOf(story.Get("title")),
			"url":         SomeOr(story.Get("url"), types.String("")), // The empty string denotes no URL.
			"score":       SomeOf(story.Get("score")),
			"by":          SomeOf(story.Get("by")),
			"time":        story.Get("time"), // This will never be Nothing.
			"descendants": SomeOf(story.Get("descendants")),
			"story":       ds.Database().WriteValue(story),
		})
	})
	close(streamData)

	fmt.Println("stream completed")

	top := <-newList

	fmt.Println("list created")

	return top
}

func update(ds datas.Dataset, old types.Value, new types.Value, dest types.Struct) types.Struct {
	// 1. Diff old and new
	// 2. For each changed id, find the changed story
	// 3. For each changed story reprocess and update the map

	oldHead := old.(types.Struct)
	oldItems := oldHead.Get("items").(types.Map)

	newHead := new.(types.Struct)
	newItems := newHead.Get("items").(types.Map)
	newTop := newHead.Get("top").(types.List)

	changes := make(chan types.ValueChanged, 5)
	stop := make(chan struct{}, 1)

	go func() {
		newItems.Diff(oldItems, changes, stop)
		close(changes)
	}()

	items := make(map[types.Number]bool)
	stories := make(map[types.Number]types.Struct)

	for change := range changes {

		id := change.V.(types.Number)

		switch change.ChangeType {
		case types.DiffChangeAdded:
			fmt.Printf("added item %d\n", int(id))
		case types.DiffChangeModified:
			fmt.Printf("modified item %d\n", int(id))
		case types.DiffChangeRemoved:
			panic(fmt.Sprintf("unexpected remove of %d", int(change.V.(types.Number))))
		default:
			panic(fmt.Sprintf("unexpected change type for %d", int(change.V.(types.Number))))
		}

		for {
			if items[id] {
				break
			}

			items[id] = true

			item := newItems.Get(id).(types.Struct)
			if item.Type().Desc.(types.StructDesc).Name == "story" {
				stories[id] = item
				break
			}

			pid, ok := item.MaybeGet("parent")
			if !ok {
				break
			}
			id = pid.(types.Number)
		}
	}

	newItem := make(chan types.Struct, 10)
	newStory := make(chan types.Value, 10)

	go func() {
		for _, item := range stories {
			newItem <- item
		}
		close(newItem)
	}()

	workerPool(10, makeStories(newItems, newItem, newStory), func() {
		close(newStory)
	})

	destStories := dest.Get("stories").(types.Map)
	for story := range newStory {
		id := story.(types.Struct).Get("id")
		destStories = destStories.Set(id, story)
	}

	dest = dest.Set("stories", destStories)
	dest = dest.Set("top", topList(ds, newTop, destStories))

	return dest
}

type StructType struct {
	t     *types.Type
	xform []int
}

type FieldType struct {
	name string
	t    *types.Type
}

type SortableFields struct {
	xform  []int
	fields []FieldType
}

func (s SortableFields) Len() int      { return len(s.xform) }
func (s SortableFields) Swap(i, j int) { s.xform[i], s.xform[j] = s.xform[j], s.xform[i] }
func (s SortableFields) Less(i, j int) bool {
	return s.fields[s.xform[i]].name < s.fields[s.xform[j]].name
}

func MakeStructType(name string, fields []FieldType) *StructType {
	xform := make([]int, len(fields))

	for idx, _ := range xform {
		xform[idx] = idx
	}

	sort.Sort(SortableFields{xform: xform, fields: fields})

	ns := make([]string, len(fields))
	ts := make([]*types.Type, len(fields))

	for to, from := range xform {
		ns[to] = fields[from].name
		ts[to] = fields[from].t
	}

	t := types.MakeStructType(name, ns, ts)

	return &StructType{t, xform}
}

func NewStructWithType(t *StructType, values types.ValueSlice) types.Value {
	v := make(types.ValueSlice, len(values))

	for to, from := range t.xform {
		v[to] = values[from]
	}

	return types.NewStructWithType(t.t, v)
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
