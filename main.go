package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"runtime/trace"
	"time"
)

func main() {
	enableDebug := flag.Bool("debug", false, "Enable HTTP profile (port 6060) and trace")
	errorsPath := flag.String("errorsPath", "./errors/", "Path to store the error logs")

	flag.Parse()

	if len(flag.Args()) < 2 {
		fmt.Println("Missing parameters...", flag.Args())
		fmt.Println("Format: ./Sequencer [-debug] [-errorsPath ./errors/] <input_warc> <output_parquet>")
		os.Exit(-1)
	}

	inputWarcFile := flag.Args()[0]
	outputParquet := flag.Args()[1]

	fmt.Println("inputFile =", inputWarcFile)

	fmt.Println("outputParquet =", outputParquet)

	fmt.Println("errorsPath =", *errorsPath)

	if *enableDebug {
		go func() {
			log.Println(http.ListenAndServe(":6060", nil))
		}()

		f, err := os.Create("trace.out")
		if err != nil {
			panic(err)
		}
		defer f.Close()

		err = trace.Start(f)
		if err != nil {
			panic(err)
		}
		defer trace.Stop()
		fmt.Println("Debug tools started")
	}


	inputFileName := path.Base(inputWarcFile)

	// Create output path
	err := os.MkdirAll(path.Dir(outputParquet), os.ModePerm)
	if err != nil {
		log.Fatalf("Unable to create the output directory: %s", err)
		panic(err)
	}

	// Create errors path
	err = os.MkdirAll(path.Dir(*errorsPath), os.ModePerm)
	if err != nil {
		log.Fatalf("Unable to create the errors directory: %s", err)
		panic(err)
	}

	start := time.Now()


	logger, err := NewLogger(inputWarcFile, *errorsPath, inputFileName)
	if err != nil {
		fmt.Println("Error in creating the log file...")
		panic(err)
	}
	go logger.run()

	PageExtractorWorker(inputWarcFile, outputParquet, logger)

	logger.quit()
	fmt.Println("Job completed in:", time.Now().Sub(start))

}