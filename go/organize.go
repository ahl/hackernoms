// Copyright 2016 Adam H. Leventhal. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	//"os"
	//"os/signal"
	"sort"

	// "github.com/zabawaba99/firego"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

// Map<number, Struct Story {
//	id Number
//	time Number XXX there's at least one with no time; I'll try to find it
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

func main() {
	fmt.Println("starting")

	source, err := spec.GetDataset("http://localhost:8000::hn")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer source.Database().Close()

	ds, err := spec.GetDataset("http://localhost:8000::hn_org")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ds.Database().Close()

	nothing := types.NewStruct("Nothing", types.StructData{})
	nothingType := nothing.Type()

	optionString := types.MakeUnionType(types.StringType, nothingType)
	optionNumber := types.MakeUnionType(types.NumberType, nothingType)
	optionBool := types.MakeUnionType(types.BoolType, nothingType)

	storyType := MakeStructType("Comment", []FieldType{
		{"id", types.NumberType},
		{"time", types.NumberType},

		{"deleted", optionBool},
		{"dead", optionBool},

		{"descendants", optionNumber},
		{"score", optionNumber},

		{"text", optionString},
		{"url", optionString},
		{"by", optionString},
	})

	s := NewStructWithType(storyType, types.ValueSlice{
		types.Number(100),
		types.Number(500),
		nothing,
		nothing,
		nothing,
		nothing,
		nothing,
		types.String("URL"),
		nothing,
	})

	fmt.Println(types.EncodedIndexValue(s))
}

type StructType struct {
	*types.Type

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

	//types.NewStructWithType

	fmt.Println(xform)
	return &StructType{t, xform}
}

func NewStructWithType(t *StructType, values types.ValueSlice) types.Value {
	v := make(types.ValueSlice, len(values))

	for to, from := range t.xform {
		v[to] = values[from]
	}

	return types.NewStructWithType(t.Type, v)
}
