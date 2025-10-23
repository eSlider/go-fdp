package binance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/duckdb/duckdb-go/v2"
)

// NewAwsConfig returns aws config for binance data
// Ultimately this is just S3 with anonymous access and ap-northeast-1 region
// Alternatives:
//   - https://data.binance.vision/?prefix=data/spot/monthly/trades/0GBNB/
//   - https://data.binance.vision.s3.amazonaws.com/
func NewAwsConfig(ctx context.Context) (*aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("ap-northeast-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           "https://s3-ap-northeast-1.amazonaws.com",
					SigningRegion: "ap-northeast-1",
				}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown service: %s", service)
		})),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Service of binance historical assets
type Service struct {
	db         *sql.DB             // DuckDB
	ctx        context.Context     // Context
	cfg        *aws.Config         // AWS Config
	client     *s3.Client          // S3 Client
	downloader *manager.Downloader // S3 Downloader
	bucket     string
	localDir   string // Local directory for downloaded files
}

// List objects by path whic
func (s *Service) List(
	path string,
	callbacks ...func(path string, page *s3.ListObjectsV2Output) error,
) (
	paths []string,
	err error,
) {

	// List all files in the bucket in the provided path

	// Create paginator
	list := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		MaxKeys: aws.Int32(100), // By default, 1000, but we need to iterate over all pages using callbacks
		Prefix:  aws.String(path),
		//StartAfter:               nil, // Optional, start after a key
	})

	// Iterate over pages
	for list.HasMorePages() {
		page, err := list.NextPage(s.ctx)
		if err != nil {
			log.Fatalf("list error: %v", err)
		}

		// Handle objects as they come extract paths
		for _, obj := range page.Contents {
			key := *obj.Key
			paths = append(paths, key)

			// Handle callbacks
			for _, callback := range callbacks {
				// Run callback in a goroutine
				go func() {
					if errC := callback(key, page); errC != nil {
						err = errors.Join(err, errC)
					}
				}()
			}

		}
	}

	return paths, err
}

func (s *Service) Download(path string, w io.WriterAt) (n int64, err error) {
	n, err = s.downloader.Download(s.ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		//IfModifiedSince:            nil,
		//IfUnmodifiedSince:          nil,
		//VersionId:                  nil,
	})
	return
}

func NewService(ctx context.Context) (*Service, error) {
	// Wal=true, shared=true, not locked
	c, err := duckdb.NewConnector("data.duckdb", nil)
	if err != nil {
		log.Fatalf("could not initialize new connector: %s", err.Error())
	}

	db := sql.OpenDB(c)
	//if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS files (
	//	id UUID DEFAULT gen_random_uuid(),
	//	name VARCHAR,
	//	created_at TIMESTAMP
	//)`); err != nil {
	//	log.Fatalf("could not create files table: %s", err.Error())
	//}

	cfg, err := NewAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(*cfg)
	downloader := manager.NewDownloader(client)

	return &Service{
		bucket:     "data.binance.vision",
		client:     client,
		downloader: downloader,
		db:         db,
		cfg:        cfg,
		ctx:        ctx,
	}, nil
}
