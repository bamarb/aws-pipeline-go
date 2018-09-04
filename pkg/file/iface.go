package file

import "io"

//File interface for working with both s3 and local files
type File interface {
	Relative() string
	Size() int64
	MD5() []byte
	Reader() (io.ReadCloser, error)
	Delete() error
	String() string
	IsDirectory() bool
}

//Filesystem is a uniform interface for dealing with both local and s3 files
type Filesystem interface {
	Files() <-chan File
	Create(src File, acl string) error
	Delete(path string) error
	Error() error
}
