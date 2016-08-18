// Copyright 2016 Adam H. Leventhal. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

// Map<number, Struct Story {
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

var nothing types.Value
var nothingType *types.Type

func init() {
	nothing = types.NewStruct("Nothing", types.StructData{})
	nothingType = nothing.Type()
}

var commentType *StructType

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <src-dataset-spec> <dst-dataset-spec>\n", os.Args[0])
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

	start, ok := ds.MaybeHeadValue()
	if !ok {
		start = types.Number(1)
	}

	newItem := make(chan types.Struct, 100)
	newStory := make(chan types.Value, 100)

	go func() {
		allItems.IterFrom(start, func(id, value types.Value) bool {
			item := value.(types.Struct)

			if item.Type().Desc.(types.StructDesc).Name == "story" {
				newItem <- item
			}

			return false
		})
		close(newItem)
	}()

	for i := 0; i < 100; i += 1 {
		go func() {
			for item := range newItem {
				id := item.Get("id")
				fmt.Println(id)

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
		}()
	}

	for story := range newStory {
		id := story.(types.Struct).Get("id")
		//fmt.Println(types.EncodedIndexValue(id))
		fmt.Println(types.EncodedIndexValue(story))

		nds, err := ds.CommitValue(id)
		if err != nil {
			panic(err)
		}
		ds = nds
	}

	fmt.Println("done")
}

func OptionGet(st types.Struct, field string) types.Value {
	value, ok := st.MaybeGet(field)
	if ok {
		return value
	} else {
		return nothing
	}
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
