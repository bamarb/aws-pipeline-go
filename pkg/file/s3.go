package file

import (
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//S3Filesystem type abstracts the buckets, prefixes and keys
type S3Filesystem struct {
	err    error
	conn   s3iface.S3API
	bucket string
	path   string
}

//S3File type heps dealing with S3Files
type S3File struct {
	conn   s3iface.S3API
	bucket string
	object *s3.Object
	path   string
	md5    []byte
}

//Relative returns the s3 file path
func (s3f *S3File) Relative() string {
	return s3f.path
}

//Size returns the size of the file, taken from metadata
func (s3f *S3File) Size() int64 {
	return *s3f.object.Size
}

//IsDirectory returns true if the file is a directory
func (s3f *S3File) IsDirectory() bool {
	return strings.HasSuffix(s3f.path, "/") && *s3f.object.Size == 0
}

//MD5 returns the md5 sum stored in metadata
func (s3f *S3File) MD5() []byte {
	if s3f.md5 == nil {
		etag := *s3f.object.ETag
		v := etag[1 : len(etag)-1]
		s3f.md5, _ = hex.DecodeString(v)
	}
	return s3f.md5
}

//Reader returns an s3 file reader
func (s3f *S3File) Reader() (io.ReadCloser, error) {
	input := s3.GetObjectInput{
		Bucket: aws.String(s3f.bucket),
		Key:    s3f.object.Key,
	}
	output, err := s3f.conn.GetObject(&input)
	if err != nil {
		return nil, err
	}
	return output.Body, err
}

//Delete deletes and s3 file
func (s3f *S3File) Delete() error {
	input := s3.DeleteObjectInput{
		Bucket: aws.String(s3f.bucket),
		Key:    s3f.object.Key,
	}
	_, err := s3f.conn.DeleteObject(&input)
	return err
}

func (s3f *S3File) String() string {
	return fmt.Sprintf("s3://%s/%s", s3f.bucket, *s3f.object.Key)
}

func (s3fs *S3Filesystem) Error() error {
	return s3fs.err
}

//Files returns  a channel which can be read to enumerate all files in a path
func (s3fs *S3Filesystem) Files() <-chan File {
	ch := make(chan File, 1000)
	stripLen := strings.LastIndex(s3fs.path, "/") + 1
	if stripLen == -1 {
		stripLen = 0
	}
	go func() {
		defer close(ch)
		truncated := true
		marker := ""
		for truncated {
			input := s3.ListObjectsInput{
				Bucket: aws.String(s3fs.bucket),
				Prefix: aws.String(s3fs.path),
				Marker: aws.String(marker),
			}
			output, err := s3fs.conn.ListObjects(&input)
			if err != nil {
				s3fs.err = err
				return
			}
			for _, c := range output.Contents {
				key := c
				relpath := (*key.Key)[stripLen:]
				ch <- &S3File{s3fs.conn, s3fs.bucket, key, relpath, nil}
				marker = *c.Key
			}
			truncated = *output.IsTruncated
		}
	}()
	return ch
}

func guessMimeType(filename string) string {
	ext := mime.TypeByExtension(filepath.Ext(filename))
	if ext == "" {
		ext = "application/binary"
	}
	return ext
}

//Create creates an s3 file object
func (s3fs *S3Filesystem) Create(src File, acl string) error {
	var fullpath string
	if s3fs.path == "" || strings.HasSuffix(s3fs.path, "/") {
		fullpath = filepath.Join(s3fs.path, src.Relative())
	} else {
		fullpath = s3fs.path
	}
	input := s3manager.UploadInput{
		ACL:    aws.String(acl),
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(fullpath),
	}

	switch t := src.(type) {
	case *S3File:
		// special case for S3File to preserve header information
		getObjectInput := s3.GetObjectInput{
			Bucket: aws.String(t.bucket),
			Key:    t.object.Key,
		}
		output, err := s3fs.conn.GetObject(&getObjectInput)
		if err != nil {
			return err
		}
		defer output.Body.Close()
		input.Body = output.Body
		// transfer existing headers across
		input.ContentType = output.ContentType
		// input.LastModified = output.LastModified
		input.StorageClass = output.StorageClass
	default:
		reader, err := src.Reader()
		if err != nil {
			return err
		}
		input.Body = reader
		defer reader.Close()
		input.ContentType = aws.String(guessMimeType(src.Relative()))
	}

	u := s3manager.NewUploaderWithClient(s3fs.conn)
	_, err := u.Upload(&input)
	return err
}

//Delete deletes and s3 file object
func (s3fs *S3Filesystem) Delete(path string) error {
	fullpath := filepath.Join(s3fs.path, path)
	input := s3.DeleteObjectInput{
		Bucket: aws.String(s3fs.bucket),
		Key:    aws.String(fullpath),
	}
	_, err := s3fs.conn.DeleteObject(&input)
	return err
}
