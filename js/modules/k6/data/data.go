// Package data implements `k6/data` js module for k6.
// This modules provide utility types to work with data in an efficient way.
package data

import (
	"errors"
	"strconv"
	"sync"

	"github.com/dop251/goja"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

type (
	// RootModule is the global module instance that will create module
	// instances for each VU.
	RootModule struct {
		shared    sharedArrays
		immutable sharedArrayBuffers
	}

	// Data represents an instance of the data module.
	Data struct {
		vu        modules.VU
		shared    *sharedArrays
		immutable *sharedArrayBuffers
	}

	sharedArrays struct {
		data map[string]sharedArray
		mu   sync.RWMutex
	}
	sharedArrayBuffers struct {
		data map[string]SharedArrayBuffer
		mu   sync.RWMutex
	}
)

var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &Data{}
)

// New returns a pointer to a new RootModule instance.
func New() *RootModule {
	return &RootModule{
		shared: sharedArrays{
			data: make(map[string]sharedArray),
		},
		immutable: sharedArrayBuffers{
			data: make(map[string]SharedArrayBuffer),
		},
	}
}

// NewModuleInstance implements the modules.Module interface to return
// a new instance for each VU.
func (rm *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &Data{
		vu:        vu,
		shared:    &rm.shared,
		immutable: &rm.immutable,
	}
}

// GetOrCreateSharedArrayBuffer fetches a shared array buffer instance with
// the provided name from the module, if it does not exist yet, it is created
// from the provided data.
//
// This method is for instance used by the init context's open method, when the
// `b` and `r` options are supplied, in order to ensure we read a binary file
// from disk only once, but allow repetitive access to its underlying data.
func (rm *RootModule) GetOrCreateSharedArrayBuffer(filename string, data []byte) SharedArrayBuffer {
	return rm.immutable.get(filename, data)
}

func (i *sharedArrayBuffers) get(filename string, data []byte) SharedArrayBuffer {
	i.mu.RLock()
	array, exists := i.data[filename]
	i.mu.RUnlock()
	if !exists {
		// If the array was not found, we need to try and
		// create it. Thus, we acquire a read/write lock,
		// which will be released at the end of this if
		// statement's scope.
		i.mu.Lock()
		defer i.mu.Unlock()

		// To ensure atomicity of our operation, and as we have
		// reacquired a lock on the data, we should double check
		// the pre-existence of the array (it might have been created
		// in the meantime).
		array, exists = i.data[filename]
		if !exists {
			array = SharedArrayBuffer{arr: data}
			i.data[filename] = array
		}
	}

	return array
}

// Exports returns the exports of the data module.
func (d *Data) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]interface{}{
			"SharedArray": d.sharedArray,
		},
	}
}

const asyncFunctionNotSupportedMsg = "SharedArray constructor does not support async functions as second argument"

// sharedArray is a constructor returning a shareable read-only array
// indentified by the name and having their contents be whatever the call returns
func (d *Data) sharedArray(call goja.ConstructorCall) *goja.Object {
	rt := d.vu.Runtime()

	if d.vu.State() != nil {
		common.Throw(rt, errors.New("new SharedArray must be called in the init context"))
	}

	name := call.Argument(0).String()
	if name == "" {
		common.Throw(rt, errors.New("empty name provided to SharedArray's constructor"))
	}
	val := call.Argument(1)

	if common.IsAsyncFunction(rt, val) {
		common.Throw(rt, errors.New(asyncFunctionNotSupportedMsg))
	}

	fn, ok := goja.AssertFunction(val)
	if !ok {
		common.Throw(rt, errors.New("a function is expected as the second argument of SharedArray's constructor"))
	}

	array := d.shared.get(rt, name, fn)
	return array.wrap(rt).ToObject(rt)
}

func (s *sharedArrays) get(rt *goja.Runtime, name string, call goja.Callable) sharedArray {
	s.mu.RLock()
	array, ok := s.data[name]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		array, ok = s.data[name]
		if !ok {
			array = getShareArrayFromCall(rt, call)
			s.data[name] = array
		}
	}

	return array
}

func getShareArrayFromCall(rt *goja.Runtime, call goja.Callable) sharedArray {
	gojaValue, err := call(goja.Undefined())
	if err != nil {
		common.Throw(rt, err)
	}
	obj := gojaValue.ToObject(rt)
	if obj.ClassName() != "Array" {
		common.Throw(rt, errors.New("only arrays can be made into SharedArray")) // TODO better error
	}
	arr := make([]string, obj.Get("length").ToInteger())

	stringify, _ := goja.AssertFunction(rt.GlobalObject().Get("JSON").ToObject(rt).Get("stringify"))
	var val goja.Value
	for i := range arr {
		val, err = stringify(goja.Undefined(), obj.Get(strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}
		arr[i] = val.String()
	}

	return sharedArray{arr: arr}
}

// SharedArrayBuffer holds the bytes of a shared array buffer.
type SharedArrayBuffer struct {
	arr []byte
}

// Wrap a SharedArray buffer with a provided JS runtime
func (sab SharedArrayBuffer) Wrap(runtime *goja.Runtime) goja.ArrayBuffer {
	return runtime.NewArrayBuffer(sab.arr)
}
