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
	Region    string `mapstructure:"region"`
	Bucket    string `mapstructure:"bucket"`
	LocalDir  string `mapstructure:"local-directory"`
	AccessKey string `mapstructure:"access-key"`
	Secret    string `mapstructure:"secret"`
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

	viper.Unmarshal(conf)

	return conf
}

// Initialization routine.
func init() {
	// Retrieve config options.
	conf = getConfig()
}

func main() {

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)

	fseventchan := make(chan string)

	go RunS3Uploader(fseventchan)

	go RunWatcher(watcher, fseventchan)

	err = watcher.Add(conf.LocalDir)
	if err != nil {
		log.Fatal(err)
	}

	<-done
}

func RunWatcher(watcher *fsnotify.Watcher, fseventchan chan<- string) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
				log.Println("created or modified file: ", event.Name)
				fseventchan <- event.Name
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}
}

func RunS3Uploader(fseventchan <-chan string) {

	// Create a single AWS session (we can re use this if we're uploading many files)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(conf.Region),
		Credentials: credentials.NewStaticCredentials(conf.AccessKey, conf.Secret, ""),
	})
	if err != nil {
		log.Fatal(err)
	}

	uploader := s3manager.NewUploader(sess)

	for {

		filepath := <-fseventchan

		// Open the file for use
		file, err := os.Open(filepath)
		if err != nil {
			exitErrorf("Unable to open file %q, %v", err)
		}

		defer file.Close()

		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(conf.Bucket),
			Key:    aws.String(filepath),
			Body:   file,
		})
		if err != nil {
			// Print the error and exit.
			exitErrorf("Unable to upload %q to %q, %v", filepath, conf.Bucket, err)
		}

		fmt.Printf("Successfully uploaded %q to %q\n", filepath, conf.Bucket)
	}
}
