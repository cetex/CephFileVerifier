package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const APP_VERSION = "0.1"
const BLOCKSIZE int64 = 1024 * 1024 * 4
const CHUNKSIZE int64 = 512

var COMP = make([]byte, BLOCKSIZE)

// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")
var path *string = flag.String("p", "./", "Path to walk")
var parallel *int = flag.Int("parallel", 10, "Number of parallel reads to do")
var log *string = flag.String("w", "", "Logfile to write to")

var PreviousRun = make(map[string]interface{})

type fInfo struct {
	path       string
	info       os.FileInfo
	readErrors int
}

type walker struct {
	FileInfo chan fInfo
}

func (w walker) walkFunc(path string, info os.FileInfo, err error) error {
	if err != nil {
		fmt.Printf("Failed to walk: %v \n", path)
		fmt.Printf("File: %v\n", info.Name())
		fmt.Printf("Error: %v", err)
	}
	if info.IsDir() {
		return nil
	}
	w.FileInfo <- fInfo{path: path, info: info}
	return nil
}

func ReadFile(path string, chunkNotifier chan<- struct{}) int {
	readErrors := 0
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}
	defer file.Close()
	for {

		// Verify offset in file
		offset, err := file.Seek(0, 1)
		if err != nil {
			panic(err)
		}
		if offset%CHUNKSIZE != 0 && offset != stat.Size() {
			panic("not on a chunk boundary!")
		}

		buf := make([]byte, CHUNKSIZE)
		n, err := file.Read(buf)
		if err == io.EOF {
			// End of file, return data.
			return readErrors
		} else if err != nil {
			panic(err)
		}
		if int64(n) != CHUNKSIZE {
			fmt.Printf("Didn't read full blocksize, expected CHUNKSIZE: %v, got: %v\n", CHUNKSIZE, n)
		}
		if bytes.Compare(buf, COMP[0:n]) == 0 {
			// first n bytes is 0, read the rest
			buf = make([]byte, BLOCKSIZE-int64(n))
			nfull, err := file.Read(buf)
			if err == io.EOF {
				// End of file, return data.
				return readErrors
			} else if err != nil {
				panic(err)
			}
			if int64(nfull) != BLOCKSIZE-CHUNKSIZE {
				fmt.Printf("Didn't read full blocksize, expected (BLOCKSIZE-CHUNKSIZE): %v, got: %v\n", BLOCKSIZE-CHUNKSIZE, nfull)
			}
			if bytes.Compare(buf, COMP[0:nfull]) == 0 {
				// Found error in file.
				fmt.Printf("Found error in file, block of %v was zeroes\n", n+nfull)
				readErrors += 1
			}
		} else {
			_, err := file.Seek(BLOCKSIZE-int64(n), 1)
			if err != nil {
				panic(err)
			}
		}
		chunkNotifier <- struct{}{}
	}
}

func FileReader(id int, info <-chan fInfo, results chan<- fInfo, chunkNotifier chan<- struct{}) {
	for {
		select {
		case data, ok := <-info:
			if !ok {
				return
			}
			readErrors := ReadFile(data.path, chunkNotifier)
			data.readErrors = readErrors
			results <- data
		}
	}
}

func Logger(results chan fInfo, log *string) {
	var file *os.File
	var err error
	if *log != "" {
		file, err = os.OpenFile(*log, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
	}
	for {
		select {
		case result, ok := <-results:
			if !ok {
				return // Channel is closed
			}
			status := ""
			if result.readErrors > 0 {
				status = fmt.Sprintf("file contained %v 4096.0k blocks of binary zeroes", result.readErrors)
			} else {
				status = "Read whole file"
			}
			logString := fmt.Sprintf("%v,%v,%v,%v\n", result.path, result.info.Size(), result.info.Size(), status)
			fmt.Print(logString)
			if *log != "" {
				file.Write([]byte(logString))
			}
		}
	}
}

func LoadPrevRun(log *string, previousRun map[string]interface{}) {

}

func ChunkCounter(ChunkNotification <-chan struct{}) {
	ticker := time.NewTicker(time.Millisecond * 1000).C
	counter := 0
	for {
		select {
		case _, ok := <-ChunkNotification:
			if !ok {
				return // Channel is closed
			}
			counter++
		case <-ticker:
			fmt.Printf("Handled %v chunks last second\n", counter)
			counter = 0
		}
	}
}

func main() {
	flag.Parse() // Scan the arguments list

	var wg sync.WaitGroup
	var lwg sync.WaitGroup

	if *versionFlag {
		fmt.Println("Version:", APP_VERSION)
		return
	}

	jobs := make(chan fInfo, *parallel)
	results := make(chan fInfo, *parallel)
	chunkNotification := make(chan struct{}, *parallel)

	for w := 1; w <= *parallel; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			FileReader(w, jobs, results, chunkNotification)
		}(w)
	}
	lwg.Add(1)
	go func() {
		Logger(results, log)
		lwg.Done()
	}()

	go ChunkCounter(chunkNotification)

	walk := walker{FileInfo: jobs}
	filepath.Walk(*path, walk.walkFunc)

	// Tell workers incoming is done and Wait for stuff to finish
	close(jobs)
	wg.Wait()
	close(results)
	lwg.Wait()
}
