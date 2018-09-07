package file

import (
	"context"
	"io"
)

//File interface for working with both s3 and local files
type File interface {
	Relative() string
	Size() int64
	MD5() []byte
	Reader() (io.ReadCloser, error)
	Delete() error
	String() string
	IsDirectory() bool
	Download(context context.Context, destDir string) error
}
