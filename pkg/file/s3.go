package file

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

//S3File type helps dealing with S3Files
type S3File struct {
	conn   s3iface.S3API
	bucket string
	object *s3.Object
	path   string
	md5    []byte
}

//NewS3File an S3File constructor
func NewS3File(conn s3iface.S3API, bucket string, obj *s3.Object) *S3File {
	_, f := path.Split(*obj.Key)
	return &S3File{conn, bucket, obj, f, nil}
}

//Relative returns the s3 file name
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

//Download  the object should auto decompress .gz files
func (s3f *S3File) Download(ctx context.Context, destDir string) error {
	destFile := path.Join(destDir, stripFileExtension(s3f.Relative()))
	writer, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer writer.Close()
	//This should decompress the gzip file
	result, err := s3f.conn.GetObjectWithContext(ctx,
		&s3.GetObjectInput{Bucket: aws.String(s3f.bucket), Key: s3f.object.Key},
	)
	if err != nil {
		return err
	}
	defer result.Body.Close()
	_, err = io.Copy(writer, result.Body)
	return err
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

func guessMimeType(filename string) string {
	ext := mime.TypeByExtension(filepath.Ext(filename))
	if ext == "" {
		ext = "application/binary"
	}
	return ext
}

func stripFileExtension(filename string) string {
	var extension = path.Ext(filename)
	return filename[:len(filename)-len(extension)]
}
