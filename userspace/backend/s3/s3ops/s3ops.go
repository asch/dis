package s3ops

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"strconv"
)

type Options struct {
	Bucket string
	Remote string
	Region string
}

type S3session struct {
	session    *session.Session
	client     *s3.S3
	options    *Options
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

func New(o *Options) *S3session {
	session := &S3session{options: o}
	session.connect()
	session.createBucket()
	return session
}

func (this *S3session) Upload(key int64, buf *[]byte) {
	_, err := this.uploader.Upload(&s3manager.UploadInput{
		Bucket: &this.options.Bucket,
		Key:    aws.String(strconv.FormatInt(key, 10)),
		Body:   bytes.NewReader(*buf),
	})
	if err != nil {
		panic(err)
	}
}

func (this *S3session) Download(key int64, buf *[]byte, rng *string) {
	b := aws.NewWriteAtBuffer(*buf)
	_, err := this.downloader.Download(b, &s3.GetObjectInput{
		Bucket: &this.options.Bucket,
		Key:    aws.String(strconv.FormatInt(key, 10)),
		Range:  rng,
	})
	if err != nil {
		panic(err)
	}
}

func (this *S3session) connect() {
	var err error
	this.session, err = session.NewSession(&aws.Config{
		Endpoint:         &this.options.Remote,
		Region:           &this.options.Region,
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	this.client = s3.New(this.session)
	this.uploader = s3manager.NewUploader(this.session)
	this.downloader = s3manager.NewDownloader(this.session)
}

func (this *S3session) createBucket() {
	this.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(this.options.Bucket),
	})

	err := this.client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(this.options.Bucket),
	})

	if err != nil {
		panic(err)
	}
}
