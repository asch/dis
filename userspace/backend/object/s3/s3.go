package s3

import (
	"bytes"
	"dis/parser"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	configSection = "backend.object.s3"
	envPrefix     = "dis_backend_object_s3"
)

var (
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
	remote     string
	region     string
)

func Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	v.BindEnv("bucket")
	v.BindEnv("region")
	v.BindEnv("remote")
	bucket = v.GetString("bucket")
	region = v.GetString("region")
	remote = v.GetString("remote")

	if bucket == "" || region == "" || remote == "" {
		panic("")
	}

	connect()
}

const keyFmt = "%08d"

func Upload(key int64, buf *[]byte) {
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: &bucket,
		Key:    aws.String(fmt.Sprintf(keyFmt, key)),
		Body:   bytes.NewReader(*buf),
	})
	if err != nil {
		panic(err)
	}
}

func Download(key int64, buf *[]byte, rng *string) {
	b := aws.NewWriteAtBuffer(*buf)
	_, err := downloader.Download(b, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String(fmt.Sprintf(keyFmt, key)),
		Range:  rng,
	})
	if err != nil {
		panic(err)
	}
}

func connect() {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         &remote,
		Region:           &region,
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	client := s3.New(sess)
	uploader = s3manager.NewUploader(sess)
	downloader = s3manager.NewDownloader(sess)

	uploader.Concurrency = 32
	s3manager.WithUploaderRequestOptions(request.Option(func(r *request.Request) {
		r.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	}))(uploader)
	downloader.Concurrency = 128

	client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	err = client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		panic(err)
	}
}
