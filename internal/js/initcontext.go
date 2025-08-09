package js

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/grafana/sobek"
	"github.com/sirupsen/logrus"

	k6data "go.k6.io/k6/internal/js/modules/k6/data"
	"go.k6.io/k6/internal/loader"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib/fsext"
)

const cantBeUsedOutsideInitContextMsg = `the "%s" function is only available in the init stage ` +
	`(i.e. the global scope), see https://grafana.com/docs/k6/latest/using-k6/test-lifecycle/ for more information`

// openImpl implements openImpl() in the init context and will read and return the
// contents of a file. If the second argument is "b" it returns an ArrayBuffer
// instance, otherwise a string representation.
func openImpl(rt *sobek.Runtime, mr *modules.ModuleResolver, fs fsext.Fs, basePWD *url.URL, filename string, args ...string) (sobek.Value, error) {
	// Strip file scheme if available as we should support only this scheme
	filename = strings.TrimPrefix(filename, "file://")
	data, err := readFile(fs, fsext.Abs(basePWD.Path, filename))
	if err != nil {
		return nil, err
	}

	if len(args) > 0 && strings.Contains(args[0], "b") {
		dataModule, exists := mr.GetGoModule("k6/data")
		if !exists {
			return nil, fmt.Errorf(
				"an internal error occurred; " +
					"reason: the data module is not loaded in the init context. " +
					"It looks like you've found a bug, please consider " +
					"filling an issue on Github: https://github.com/grafana/k6/issues/new/choose",
			)
		}
		if strings.Contains(args[0], "r") {
			// We ask the data module to get or create a shared array buffer entry from
			// its internal mapping using the provided filename, and data.
			//
			// N.B: using mmap in read-only mode could be a better option, rather than
			// loading all the data in memory; as it's essentially the mmap syscall's
			// reason to be. However mmap is tricky
			// in Go: https://valyala.medium.com/mmap-in-go-considered-harmful-d92a25cb161d
			// Also, mmap is essentially a Unix syscall and we are not sure about the state
			// of its integration in Windows and MacOS. As of december 2021, https://github.com/edsrzf/mmap-go
			// would look like the best portable solution if we were to take that route.
			sharedArrayBuffer := dataModule.(*k6data.RootModule).GetOrCreateSharedArrayBuffer(filename, data)
			ab := sharedArrayBuffer.Wrap(rt)
			return rt.ToValue(&ab), nil
		}
		ab := rt.NewArrayBuffer(data)
		return rt.ToValue(&ab), nil
	}
	return rt.ToValue(string(data)), nil
}

func readFile(fileSystem fsext.Fs, filename string) (data []byte, err error) {
	defer func() {
		if errors.Is(err, fsext.ErrPathNeverRequestedBefore) {
			// loading different files per VU is not supported, so all files should are going
			// to be used inside the scenario should be opened during the init step (without any conditions)
			err = fmt.Errorf(
				"open() can't be used with files that weren't previously opened during initialization (__VU==0), path: %q",
				filename,
			)
		}
	}()

	// Workaround for https://github.com/spf13/fsext/issues/201
	if isDir, err := fsext.IsDir(fileSystem, filename); err != nil {
		return nil, err
	} else if isDir {
		return nil, fmt.Errorf("open() can't be used with directories, path: %q", filename)
	}

	return fsext.ReadFile(fileSystem, filename)
}

// allowOnlyOpenedFiles enables seen only files
func allowOnlyOpenedFiles(fs fsext.Fs) {
	alreadyOpenedFS, ok := fs.(fsext.OnlyCachedEnabler)
	if !ok {
		return
	}

	alreadyOpenedFS.AllowOnlyCached()
}

func generateSourceMapLoader(logger logrus.FieldLogger, filesystems map[string]fsext.Fs,
) func(path string) ([]byte, error) {
	return func(path string) ([]byte, error) {
		u, err := url.Parse(path)
		if err != nil {
			return nil, err
		}
		data, err := loader.Load(logger, filesystems, u, path)
		if err != nil {
			return nil, err
		}
		return data.Data, nil
	}
}
