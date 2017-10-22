/*
 * Copyright (C) 2017 Andy Kimball
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tscache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

const (
	arenaSize = 64 * 1024 * 1024
)

func TestCacheAdd(t *testing.T) {
	ts := hlc.Timestamp{100, 100}
	ts2 := hlc.Timestamp{200, 201}

	c := New(arenaSize)
	c.Add([]byte("apricot"), ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("banana")))

	c.Add([]byte("banana"), ts2)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("cherry")))
}

func TestCacheSingleRange(t *testing.T) {
	ts := hlc.Timestamp{100, 100}
	ts2 := hlc.Timestamp{200, 50}

	c := New(arenaSize)

	c.AddRange([]byte("apricot"), []byte("orange"), 0, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("raspberry")))

	// Try again and make sure it's a no-op.
	c.AddRange([]byte("apricot"), []byte("orange"), 0, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("raspberry")))

	// Ratchet up the timestamps.
	c.AddRange([]byte("apricot"), []byte("orange"), 0, ts2)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("raspberry")))

	// Add disjoint range.
	c.AddRange([]byte("pear"), []byte("tomato"), ExcludeFrom|ExcludeTo, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("peach")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("watermelon")))

	// Try again and make sure it's a no-op.
	c.AddRange([]byte("pear"), []byte("tomato"), ExcludeFrom|ExcludeTo, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("peach")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamps.
	c.AddRange([]byte("pear"), []byte("tomato"), ExcludeFrom|ExcludeTo, ts2)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("peach")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("watermelon")))
}

func TestCacheOpenRanges(t *testing.T) {
	floorTs := hlc.Timestamp{100, 0}
	ts := hlc.Timestamp{200, 200}
	ts2 := hlc.Timestamp{200, 201}

	c := New(arenaSize)
	c.floorTs = floorTs

	c.AddRange([]byte("banana"), nil, ExcludeFrom, ts)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("orange")))

	c.AddRange([]byte(""), []byte("kiwi"), 0, ts2)
	require.Equal(t, ts2, c.LookupTimestamp(nil))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("orange")))
}

func TestCacheSupersetRange(t *testing.T) {
	floorTs := hlc.Timestamp{100, 0}
	ts := hlc.Timestamp{200, 1}
	ts2 := hlc.Timestamp{201, 0}
	ts3 := hlc.Timestamp{300, 0}

	c := New(arenaSize)
	c.floorTs = floorTs

	// Same range.
	c.AddRange([]byte("kiwi"), []byte("orange"), 0, ts)
	c.AddRange([]byte("kiwi"), []byte("orange"), 0, ts2)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("mango")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("raspberry")))

	// Superset range, but with lower timestamp.
	c.AddRange([]byte("grape"), []byte("pear"), 0, ts)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("watermelon")))

	// Superset range, but with higher timestamp.
	c.AddRange([]byte("banana"), []byte("raspberry"), 0, ts3)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("watermelon")))
}

func TestCacheContiguousRanges(t *testing.T) {
	floorTs := hlc.Timestamp{WallTime: 100, Logical: 0}
	ts := hlc.Timestamp{200, 1}
	ts2 := hlc.Timestamp{201, 0}

	c := New(arenaSize)
	c.floorTs = floorTs

	c.AddRange([]byte("banana"), []byte("kiwi"), ExcludeTo, ts)
	c.AddRange([]byte("kiwi"), []byte("orange"), ExcludeTo, ts2)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("orange")))
}

func TestCacheOverlappingRanges(t *testing.T) {
	floorTs := hlc.Timestamp{WallTime: 100, Logical: 0}
	ts := hlc.Timestamp{200, 1}
	ts2 := hlc.Timestamp{201, 0}
	ts3 := hlc.Timestamp{300, 0}
	ts4 := hlc.Timestamp{400, 0}

	c := New(arenaSize)
	c.floorTs = floorTs

	c.AddRange([]byte("banana"), []byte("kiwi"), 0, ts)
	c.AddRange([]byte("grape"), []byte("raspberry"), ExcludeTo, ts2)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("raspberry")))

	c.AddRange([]byte("apricot"), []byte("orange"), 0, ts3)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts2, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("raspberry")))

	c.AddRange([]byte("kiwi"), []byte(nil), ExcludeFrom, ts4)
	require.Equal(t, floorTs, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, ts3, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts4, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, ts4, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, ts4, c.LookupTimestamp([]byte("raspberry")))
}

func TestCacheBoundaryRange(t *testing.T) {
	ts := hlc.Timestamp{100, 100}

	c := New(arenaSize)

	// Don't allow nil from key.
	require.Panics(t, func() { c.AddRange([]byte(nil), []byte(nil), ExcludeFrom, ts) })

	// If from key is greater than to key, then range is zero-length.
	c.AddRange([]byte("kiwi"), []byte("apple"), 0, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("raspberry")))

	// If from key is same as to key, and both are excluded, then range is
	// zero-length.
	c.AddRange([]byte("banana"), []byte("banana"), ExcludeFrom|ExcludeTo, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("kiwi")))

	// If from key is same as to key, then range has length one.
	c.AddRange([]byte("mango"), []byte("mango"), 0, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("mango")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("orange")))

	// If from key is same as to key, then range has length one.
	c.AddRange([]byte("banana"), []byte("banana"), ExcludeTo, ts)
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, ts, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, hlc.Timestamp{}, c.LookupTimestamp([]byte("cherry")))
}

func TestCacheFillCache(t *testing.T) {
	const n = 200

	// Use constant seed so that skiplist towers will be of predictable size.
	rand.Seed(0)

	c := New(3000)

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		c.AddRange(key, key, 0, hlc.Timestamp{WallTime: int64(100 + i), Logical: int32(i)})
	}

	floorTs := c.floorTs
	require.True(t, hlc.Timestamp{WallTime: 100}.Less(floorTs))

	lastKey := []byte(fmt.Sprintf("%05d", n-1))
	require.Equal(t, hlc.Timestamp{WallTime: int64(100 + n - 1), Logical: int32(n - 1)}, c.LookupTimestamp(lastKey))

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		require.False(t, c.LookupTimestamp(key).Less(floorTs))
	}
}

// Repeatedly fill cache and make sure timestamp lookups always increase.
func TestCacheFillCache2(t *testing.T) {
	const n = 10000

	c := New(997)
	key := []byte("some key")

	for i := 0; i < n; i++ {
		ts := hlc.Timestamp{WallTime: int64(i), Logical: int32(i)}
		c.Add(key, ts)
		require.True(t, !c.LookupTimestamp(key).Less(ts))
	}
}

// Test concurrency with a small cache size in order to force lots of cache
// rotations.
func TestCacheConcurrencyRotates(t *testing.T) {
	testConcurrency(t, 2048)
}

// Test concurrency with a larger cache size in order to test slot concurrency
// without the added complication of cache rotations.
func TestCacheConcurrencySlots(t *testing.T) {
	testConcurrency(t, arenaSize)
}

func BenchmarkAdd(b *testing.B) {
	const max = 500000000

	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	cache := New(64 * 1024 * 1024)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	size := 1
	for i := 0; i < 9; i++ {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				rnd := int64(rng.Int31n(max))
				from := []byte(fmt.Sprintf("%020d", rnd))
				to := []byte(fmt.Sprintf("%020d", rnd+int64(size-1)))
				cache.AddRange(from, to, 0, clock.Now())
			}
		})

		size *= 10
	}
}

func BenchmarkCache(b *testing.B) {
	const parallel = 1
	const max = 1000000000
	const data = 500000

	cache := New(arenaSize)
	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < data; i++ {
		from, to := makeRange(rng.Int31n(max))
		cache.AddRange(from, to, ExcludeFrom|ExcludeTo, clock.Now())
	}

	for i := 0; i <= 10; i++ {
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			var wg sync.WaitGroup

			for p := 0; p < parallel; p++ {
				wg.Add(1)

				go func(i int) {
					defer wg.Done()

					rng := rand.New(rand.NewSource(time.Now().UnixNano()))

					for n := 0; n < b.N/parallel; n++ {
						readFrac := rng.Int31()
						keyNum := rng.Int31n(max)

						if (readFrac % 10) < int32(i) {
							key := []byte(fmt.Sprintf("%020d", keyNum))
							cache.LookupTimestamp(key)
						} else {
							from, to := makeRange(keyNum)
							cache.AddRange(from, to, ExcludeFrom|ExcludeTo, clock.Now())
						}
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

func testConcurrency(t *testing.T, cacheSize uint32) {
	const n = 10000
	const slots = 20

	var wg sync.WaitGroup
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	cache := New(cacheSize)

	for i := 0; i < slots; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			key := []byte(fmt.Sprintf("%05d", i))
			maxTs := hlc.Timestamp{}

			for j := 0; j < n; j++ {
				fromNum := rng.Intn(slots)
				toNum := rng.Intn(slots)

				if fromNum > toNum {
					fromNum, toNum = toNum, fromNum
				} else if fromNum == toNum {
					toNum++
				}

				from := []byte(fmt.Sprintf("%05d", fromNum))
				to := []byte(fmt.Sprintf("%05d", toNum))

				now := clock.Now()
				cache.AddRange(from, to, 0, now)

				ts := cache.LookupTimestamp(from)
				require.False(t, ts.Less(now))

				ts = cache.LookupTimestamp(to)
				require.False(t, ts.Less(now))

				ts = cache.LookupTimestamp(key)
				require.False(t, ts.Less(maxTs))
				maxTs = ts
			}
		}(i)
	}

	wg.Wait()
}

func makeRange(start int32) (from, to []byte) {
	var end int32

	rem := start % 100
	if rem < 80 {
		end = start + 0
	} else if rem < 90 {
		end = start + 100
	} else if rem < 95 {
		end = start + 10000
	} else if rem < 99 {
		end = start + 1000000
	} else {
		end = start + 100000000
	}

	from = []byte(fmt.Sprintf("%020d", start))
	to = []byte(fmt.Sprintf("%020d", end))
	return
}
