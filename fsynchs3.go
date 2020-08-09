package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type config struct {
	Region    string `yaml:"region"`
	Bucket    string `yaml:"bucket"`
	LocalDir  string `yaml:"local-directory"`
	AccessKey string `yaml:"access-key"`
	Secret    string `yaml:"secret"`
}

var (
	conf *config
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func getConfig() *config {

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	conf := &config{}

	conf.Region = viper.GetString("region")
	conf.Bucket = viper.GetString("bucket")
	conf.LocalDir = viper.GetString("local-directory")
	conf.AccessKey = viper.GetString("access-key")
	conf.Secret = viper.GetString("secret")

	return conf
}

// Initialization routine.
func init() {
	// Retrieve config options.
	conf = getConfig()
}

func main() {
	// Create a single AWS session (we can re use this if we're uploading many files)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(conf.Region),
		Credentials: credentials.NewStaticCredentials(conf.AccessKey, conf.Secret, ""),
	})
	if err != nil {
		log.Fatal(err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
					log.Println("created or modified file: ", event.Name)

					AddFileToS3(sess, conf.Bucket, event.Name)
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(conf.LocalDir)
	if err != nil {
		log.Fatal(err)
	}

	<-done
}

func AddFileToS3(sess *session.Session, bucket string, filepath string) {

	defer timeTrack(time.Now(), "AddFileToS3")

	// Open the file for use
	file, err := os.Open(filepath)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", err)
	}

	defer file.Close()

	uploader := s3manager.NewUploader(sess)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath),
		Body:   file,
	})
	if err != nil {
		// Print the error and exit.
		exitErrorf("Unable to upload %q to %q, %v", filepath, bucket, err)
	}

	fmt.Printf("Successfully uploaded %q to %q\n", filepath, bucket)
}
