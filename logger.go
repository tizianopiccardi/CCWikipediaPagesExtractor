package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"sync"
)

type Exception struct {
	//File            string
	SourcePage      string
	ErrorType       string
	Message         string
	OriginalMessage string
}

type Logger struct {

	WarcPath string

	ErrorsFilePath   string
	ErrorsFileName   string

	ErrorsGZipFileWriter *gzip.Writer
	ErrorsFileWriter *bufio.Writer

	Exceptions chan Exception

	writingMutex sync.Mutex
}

func NewLogger(warcPath string, errorsPath string, errorsFileName string) (Logger, error) {
	logger := Logger{WarcPath: warcPath, ErrorsFilePath: errorsPath, ErrorsFileName: errorsFileName}

	logFile, err := os.Create(errorsPath+errorsFileName+".json.gz")
	if err != nil {
		return logger, err
	}

	logger.ErrorsGZipFileWriter = gzip.NewWriter(logFile)
	logger.ErrorsFileWriter = bufio.NewWriter(logger.ErrorsGZipFileWriter)
	logger.Exceptions = make(chan Exception, 100)
	return logger, nil
}

func (logger Logger) quit() {
	logger.writingMutex.Lock()
	logger.ErrorsFileWriter.Flush()
	logger.ErrorsGZipFileWriter.Close()
	logger.writingMutex.Unlock()
}

func (logger Logger) run() {

	for {
		select {
		case e := <-logger.Exceptions:
			logger.writingMutex.Lock()
			//e.File = logger.WarcPath
			jsonError, _ := json.Marshal(e)
			logger.ErrorsFileWriter.Write(jsonError)
			logger.ErrorsFileWriter.Write([]byte("\n"))
			logger.writingMutex.Unlock()
		}
	}
}
