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
	//ii.nextItem = ii.maxItem - 10

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
func (ii *ItemIterator) Next() uint32 {
	ii.lock.Lock()
	defer ii.lock.Unlock()

	for {
		if ll := len(ii.extraItems); ll != 0 {
			item := ii.extraItems[ll-1]
			ii.extraItems = ii.extraItems[:ll-1]
			return item
		}

		if ii.nextItem <= ii.maxItem {
			item := ii.nextItem
			ii.nextItem += 1
			return item
		}

		ii.cv.Wait()
	}
}

func (ii *ItemIterator) Max() uint32 {
	return ii.maxItem
}

// Replace the new value in a chan without blocking.
func poke(value uint64, ch chan uint64) {
	select {
	case ch <- value:
	default:
		// Pull the old value out. This can race, so we need to take care not to block.
		select {
		case there := <-ch:
			if value < there {
				value = there
			}
		default:
		}
		select {
		case ch <- value:
		default: // While we've been screwing around someone else snuck their value in; fine.
		}
	}
}

type datum struct {
	id    uint64
	key   types.Number
	value types.Struct
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

	//newMap := make(chan types.Map, 1)
	depth := 150
	newData := make(chan datum, depth)
	streamData := make(chan types.Value, depth)

	//fmt.Println(runtime.GOMAXPROCS(0))

	start := time.Now()

	newMap := types.NewStreamingMap(ds.Database(), streamData)

	// Grab the data from hacker news.
	for i := 0; i < 500; i++ {
		go churn(newData, ii)
	}

	var count uint32
	done := false
	for !done {
		select {
		case d := <-newData:
			count += 1

			total := ii.Max()

			dur := time.Since(start)

			eta := time.Duration(float64(dur) * float64(total-count) / float64(count))

			if count%1000 == 0 {
				fmt.Printf("sent:  %d/%d %s\n", count, total, eta)
			}

			streamData <- d.key
			streamData <- d.value

			// At some point we'll decide he's too close for missiles and switch to guns. At that point we'll close the streamData chan, and break out of this loop. There may be data left in newData, but we'll pick that up later.
			if count > total-uint32(depth)*2 {
				// if count > 100000 {
				close(streamData)
				done = true
			}
		}
	}

	fmt.Println("generating map...")

	// Wait for the map to build; this could take quite a long time so we need to figure out a plan for buffering data in the meantime. The most critical thing to stay on top of is going to be "extra" items in the iterator.
	mm := <-newMap
	fmt.Println("first commit...")
	nds, err := ds.CommitValue(mm)
	if err != nil {
		panic(err)
	}
	ds = nds
	fmt.Println("processing...")

	for {
		d := <-newData

		mm = mm.Set(d.key, d.value)

		last := time.Now()
		blocked := false
		for !blocked && time.Since(last) < time.Second {
			select {
			case d = <-newData:
				count += 1
				mm = mm.Set(d.key, d.value)
			default:
				blocked = true
			}
		}

		total := ii.Max()

		dur := time.Since(start)

		eta := time.Duration(float64(dur) * float64(total-count) / float64(count))

		fmt.Printf("sent:  %d/%d %s\n", count, total, eta)

		nds, err := ds.CommitValue(mm)
		if err != nil {
			panic(err)
		}
		ds = nds
	}

	/*
		// Build the map.
		go func() {
			mm := types.NewMap()
			buffer := make([]types.Value, 0, 200)
			for {
				buffer = buffer[:0]

				st := <-newData
				buffer = append(buffer, st.Get("id"), st)

				done := false
				for len(buffer) < cap(buffer) && !done {
					select {
					case st = <-newData:
						buffer = append(buffer, st.Get("id"), st)
					default:
						done = true
					}
				}

				//n := uint32(id.(types.Number))
				//fmt.Println(n)
				delta := Measure(func() {
					mm = mm.SetM(buffer...)
				})

				dist.Add(uint64(delta))

				_ = start

				// Make sure the latest map is sitting in the chan.
				select {
				case newMap <- mm:
				default:
					select {
					case _ = <-newMap:
					default:
					}
					select {
					case newMap <- mm:
					default:
						panic("shouldn't be able to block")
					}
				}

				//fmt.Printf("built: %d/%d\n", mm.Len(), ii.Max())
			}
		}()

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)

		// Commit to Noms.
		for {
			iter := time.Now()
			select {
			case mm := <-newMap:
				nds, err := ds.CommitValue(mm)
				if err != nil {
					panic(err)
				}
				ds = nds

				total := ii.Max()
				done := mm.Len()

				d := time.Since(start)

				eta := time.Duration(float64(d) * float64(total) / float64(done))

				fmt.Printf("sent:  %d/%d %s %.2f %.2f\n", done, total, eta, dist.Avg(), dist.StdDev())
				dist.Hist()
			case _ = <-sig:
				fmt.Println("exiting...")
				os.Exit(0)
			}

			time.Sleep(time.Second - time.Since(iter))
		}
	*/
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
		item := ii.Next()
		url := fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d", item)
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
					id:    uint64(val["id"].(float64)),
					key:   data["id"].(types.Number),
					value: st,
				}

				newData <- d
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
