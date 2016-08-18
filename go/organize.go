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

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <src-dataset-spec> <dst-dataset-spec>\n", path.Base(os.Args[0]))
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		return
	}

	source, err := spec.GetDataset(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}
	defer source.Database().Close()

	ds, err := spec.GetDataset(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ds.Database().Close()

	fmt.Println("starting")

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

	storyType := MakeStructType("Story", []FieldType{
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

	head := source.HeadValue().(types.Struct)
	allItems := head.Get("items").(types.Map)

	/*
		start, ok := ds.MaybeHeadValue()
		if !ok {
			start = types.Number(1)
		}
	*/

	newItem := make(chan types.Struct, 100)
	newStory := make(chan types.Value, 100)

	lastKey, _ := allItems.Last()
	lastIndex := int(lastKey.(types.Number))

	go func() {
		allItems.Iter(func(id, value types.Value) bool {
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

	workerPool(50, func() {
		for item := range newItem {
			id := item.Get("id")

			// Known stubs with just id and type
			if fields := item.ChildValues(); len(fields) == 2 {
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
	}, func() {
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
			dur := time.Since(start)
			eta := dur * time.Duration(float64(lastIndex-count)/float64(count))
			fmt.Printf("%d/%d %s\n", int(id.(types.Number)), lastIndex, eta)
		}

		streamData <- id
		streamData <- story
	}
	close(streamData)

	fmt.Println("stream completed")

	stories := <-newMap

	fmt.Println("map created")

	topStories := head.Get("top").(types.List)

	streamData = make(chan types.Value, 100)
	newList := types.NewStreamingList(ds.Database(), streamData)

	topStories.IterAll(func(item types.Value, _ uint64) {
		id := item.(types.Number)
		v, ok := stories.MaybeGet(id)
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

	ds, err = ds.CommitValue(types.NewStruct("HackerNoms", types.StructData{
		"stories": stories,
		"top":     top,
		"head":    types.String(head.Hash().String()),
	}))
	if err != nil {
		panic(err)
	}

	fmt.Println("done!")
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
				panic(fmt.Sprintf("unable to look up %d", int(id.(types.Number))))
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
