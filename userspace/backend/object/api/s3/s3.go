package s3

import (
	"bufio"
	"bytes"
	"dis/parser"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"strconv"
	"time"
)

const (
	configSection = "backend.object.s3"
	envPrefix     = "dis_backend_object_s3"
)

var (
	uploader      *s3manager.Uploader
	downloader    *s3manager.Downloader
	client        *s3.S3
	bucket        string
	remote        string
	region        string
	FnHeaderToMap func(header *[]byte, key, size int64)
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
	var err error
	for i := 0; i < 200; i++ {
		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    aws.String(fmt.Sprintf(keyFmt, key)),
			Body:   bytes.NewReader(*buf),
		})
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i) * time.Millisecond)
	}
	if err != nil {
		panic(err)
	}
}

func Download(key int64, buf *[]byte, from, to int64) {
	if to-from+1 != int64(len(*buf)) {
		panic("")
	}
	rng := fmt.Sprintf("bytes=%d-%d", from, to)
	b := aws.NewWriteAtBuffer(*buf)
	var err error
	for i := 0; i < 200; i++ {
		_, err = downloader.Download(b, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    aws.String(fmt.Sprintf(keyFmt, key)),
			Range:  &rng,
		})
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i) * time.Millisecond)
	}
	if err != nil {
		panic(err)
	}
}

func Delete(key int64) {
	_, err := client.DeleteObject(&s3.DeleteObjectInput{Bucket: &bucket, Key: aws.String(fmt.Sprintf(keyFmt, key))})
	if err != nil {
		fmt.Println(err)
	}
}

func Void(key int64) {
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: &bucket,
		Key:    aws.String(fmt.Sprintf(keyFmt, key)),
		Body:   bytes.NewReader(make([]byte, 0)),
	})
	if err != nil {
		fmt.Println(err)
	}
}

func connect() {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:                      &remote,
		Region:                        &region,
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	client = s3.New(sess)
	uploader = s3manager.NewUploader(sess)
	downloader = s3manager.NewDownloader(sess)

	uploader.Concurrency = 1
	s3manager.WithUploaderRequestOptions(request.Option(func(r *request.Request) {
		r.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	}))(uploader)
	downloader.Concurrency = 1

	_, err = client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		fmt.Println("Do you want to recover volume from", bucket, "? [Y/n]")
		yn, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if yn == "N\n" || yn == "n\n" {
			err = client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
				Bucket: &bucket,
			}, func(page *s3.ListObjectsV2Output, last bool) bool {
				for _, o := range page.Contents {
					client.DeleteObject(&s3.DeleteObjectInput{Bucket: &bucket, Key: o.Key})
				}
				return true
			})
			if err != nil {
				panic(err)
			}

			_, err = client.DeleteBucket(&s3.DeleteBucketInput{Bucket: &bucket})
			if err != nil {
				panic(err)
			}
			_, err = client.CreateBucket(&s3.CreateBucketInput{Bucket: &bucket})
			if err != nil {
				panic(err)
			}
			err = client.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &bucket})
			if err != nil {
				panic(err)
			}
			return
		}

		var lastKey int64 = -1
		var finished bool
		err = client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		}, func(page *s3.ListObjectsV2Output, last bool) bool {
			for _, o := range page.Contents {
				key, _ := strconv.ParseInt(*o.Key, 10, 64)
				if finished {
					Delete(key)
					continue
				}
				if lastKey != -1 && key != lastKey+1 {
					finished = true
					continue
				}
				lastKey = key
				if *o.Size == 0 {
					continue
				}
				headerSize := (*o.Size / 512) * 16
				buf := make([]byte, headerSize)
				Download(key, &buf, 0, headerSize-1)
				FnHeaderToMap(&buf, key, *o.Size)
			}
			return true
		})
		if err != nil {
			panic(err)
		}
	} else {
		var err error
		for i := 0; i < 200; i++ {
			_, err = client.CreateBucket(&s3.CreateBucketInput{Bucket: &bucket})
			if err == nil {
				break
			}
			time.Sleep(time.Duration(i) * time.Millisecond)
		}
		if err != nil {
			panic(err)
		}
		err = client.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &bucket})
		if err != nil {
			panic(err)
		}
	}
}
