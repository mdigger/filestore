package httpfs

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Store represents the file store.
type Store struct {
	root string // absolute file store root path
}

// Open opens a file store at the specified path. If this directory does
// not exist, then create it. Returns an error if unable to open or create
// directory.
func Open(root string) (*Store, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(root, 0700)
	if err != nil {
		return nil, err
	}
	return &Store{root}, nil
}

// Path returns the root path of the files store.
func (s Store) Path() string {
	return s.root
}

const filepathSeparator = string(filepath.Separator)

// absPath returns the absolute path to the file with the specified name in the
// file store. Returns the os.ErrPermission if the resulting path is outside the
// store.
func (s *Store) absPath(name string) (string, error) {
	name = filepath.FromSlash(name)
	if strings.HasPrefix(name, filepathSeparator) {
		name = strings.TrimPrefix(name, filepathSeparator)
	}
	parts := strings.SplitN(name, filepathSeparator, 4)
	if len(parts) > 3 {
		parts = parts[:3]
	}
	name = strings.Join(parts, filepathSeparator)
	name = filepath.Join(s.root, name)
	name = filepath.Clean(name)
	if !strings.HasPrefix(name, s.root) {
		return "", os.ErrPermission
	}
	return name, nil
}

// Open returns an open file with the specified name from the file store.
func (s *Store) Open(name string) (*os.File, error) {
	pathName, err := s.absPath(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	file, err := os.Open(pathName)
	if err != nil {
		err.(*os.PathError).Path = name
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		err.(*os.PathError).Path = name
		return nil, err
	}
	if fi.IsDir() { // open only files, not directories
		_ = file.Close()
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrPermission}
	}
	// update access time
	now := time.Now()
	_ = os.Chtimes(pathName, now, now)
	return file, nil
}

// ServeFile gives a file with the specified name in response to a HTTP request.
// Returns an error if a file with this name not found in the store.
func (s *Store) ServeFile(w http.ResponseWriter, r *http.Request,
	name string) error {
	file, err := s.Open(name)
	if err != nil {
		return err
	}
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		err.(*os.PathError).Path = name
		return err
	}

	http.ServeContent(w, r, "", fi.ModTime(), file)
	_ = file.Close()
	return nil
}

// Remove removes a file with the specified name from the files store.
func (s *Store) Remove(name string) error {
	pathName, err := s.absPath(name)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	err = os.Remove(pathName)
	if err != nil {
		err.(*os.PathError).Path = name
		return err
	}

	// trying to remove the directories that are used when storing the file,
	// if they are empty
	for i := 0; i < 2; i++ {
		pathName = filepath.Dir(pathName)
		err = os.Remove(pathName)
		if err != nil {
			break
		}
	}

	return nil
}

// Create creates a new file in the file store and returns information about it.
// The file name is generated based on the two hash sums, that is a sufficient
// guarantee of its uniqueness to have different content.
func (s *Store) Create(r io.Reader) (*FileInfo, error) {
	const tmpfileName = "<temporary file>"
	tmpfile, err := ioutil.TempFile(s.root, "~tmp")
	if err != nil {
		err.(*os.PathError).Path = tmpfileName
		return nil, err
	}
	defer os.Remove(tmpfile.Name()) // clean up

	fileInfo, err := hashCopy(tmpfile, r) // copy & hash
	if err != nil {
		_ = tmpfile.Close()
		return nil, &os.PathError{Op: "create", Path: tmpfileName, Err: err}
	}

	if err = tmpfile.Close(); err != nil {
		if e, ok := err.(*os.PathError); ok {
			e.Path = tmpfileName
		}
		return nil, err
	}

	name := fileInfo.Name            // relative files store path
	pathName, err := s.absPath(name) // absolute path in files store
	if err != nil {
		return nil, &os.PathError{Op: "create", Path: name, Err: err}
	}

	// if this file already exists, trying to set the current time of creation
	now := time.Now()
	err = os.Chtimes(pathName, now, now)
	if err == nil {
		// this file already exists and we have updated it time
		return fileInfo, nil
	}
	// there is no such file - create a directory to store the file
	err = os.MkdirAll(filepath.Dir(pathName), 0700)
	if err != nil {
		err.(*os.PathError).Path = name
		return nil, err
	}
	// rename temporary file to new name
	err = os.Rename(tmpfile.Name(), pathName)
	if err != nil {
		err.(*os.PathError).Path = name
		return nil, err
	}

	return fileInfo, nil
}

// Clean removes all files whose creation time is less than a specified
// lifetime. Removes all files if the lifetime is less than or equal to zero.
func (s *Store) Clean(lifetime time.Duration) error {
	if lifetime <= 0 {
		path := s.root
		// if !strings.HasSuffix(path, "/") {
		// 	path += "/"
		// }
		return os.RemoveAll(path)
	}

	// last valid date and time
	valid := time.Now().Add(-lifetime)
	err := filepath.Walk(s.root,
		func(filename string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			// don't remove folders
			if info.IsDir() {
				return nil
			}
			// don't remove new files
			mtime := info.ModTime()
			if mtime.After(valid) {
				return nil
			}
			// trying to remove file if it outdated
			err = os.Remove(filename)
			if err != nil {
				return nil // skip removing error
			}
			// trying to remove the directories that are used when storing the
			// file, if they are empty
			for i := 0; i < 2; i++ {
				filename = filepath.Dir(filename)
				err = os.Remove(filename)
				if err != nil {
					break
				}
			}
			return nil
		})

	if os.IsNotExist(err) {
		return nil
	}
	return err
}
