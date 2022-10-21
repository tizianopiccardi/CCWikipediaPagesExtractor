package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/PuerkitoBio/purell"
	"github.com/slyrz/warc"
	"github.com/tevino/abool"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
)

const CHUNK_SIZE = 500000

var whitelist = map[string]bool{
	"wikipedia": true,
	"wikiquote": true,
	"wikinews": true,
	"wikiversity": true,
	"wiktionary": true,
	"wikisource": true,
	"wikibooks": true,
	"wikivoyage": true,
}

const PURELL_FLAGS = purell.FlagsUsuallySafeGreedy |
	purell.FlagForceHTTP |
	purell.FlagRemoveFragment |
	purell.FlagSortQuery

func isWikimediaProject(pageHost string) bool {
	hostParts := strings.Split(pageHost, ":")
	pageHostParts := strings.Split(hostParts[0], ".")

	if len(pageHostParts)>=2 && pageHostParts[len(pageHostParts)-1]=="org"{
		domainName := pageHostParts[len(pageHostParts)-2]
		if _, ok := whitelist[domainName]; ok {
			//fmt.Println(pageHost)
			return true
		}
	}
	return false
}

func isWikimediaProjectFromURL(pageURL string) bool {
	u, err := url.Parse(pageURL)
	if err != nil {
		return false
	}
	return isWikimediaProject(u.Host)
}

func getCharsetReader(reader *bufio.Reader, contentType string) io.Reader {
	bodySample, _ := reader.Peek(1024)
	encoding, _, _ := charset.DetermineEncoding(bodySample, contentType)
	return encoding.NewDecoder().Reader(reader)
}


func sanitizeString(rawUrl string) string {
	hrefValue := strings.Replace(strings.TrimSpace(rawUrl), "\n", "", -1)
	hrefValue = strings.Replace(hrefValue, "\t", "", -1)
	hrefValue = strings.Replace(hrefValue, "\r", "", -1)
	hrefValue = strings.Replace(hrefValue, "\u0008", "", -1)
	return hrefValue
}

func PageExtractorWorker(inputWarcFile string, outputParquet string, logger Logger) {

	fileReader, err := os.Open(inputWarcFile)
	if err != nil {
		logger.Exceptions <- Exception{
			ErrorType:       "File not found",
			Message:         inputWarcFile,
			OriginalMessage: err.Error(),
		}
		panic(err)

	} else {

		recordsReader, err := warc.NewReader(fileReader)
		if err != nil {
			logger.Exceptions <- Exception{
				//File:            sourceFile,
				//Source:          exceptionsSource,
				ErrorType:       "WARC Reader failed",
				Message:         inputWarcFile,
				OriginalMessage: err.Error(),
			}
			panic(err)

		} else {

			// Channel to share the chucks to write
			writerChannel := make(chan *PagesList, 150)

			// Synchronized boolean var to inform the reader if the writer failed
			failedWriterFlag := abool.New()

			// Get a message when the writer completed the job
			writerDone := make(chan bool)

			// - The writer runs waiting from links chunks from the channel
			// - If it fails, it sets the failedWriterFlag to TRUE and log the error
			// - The reader checks regularly the flag, if it's TRUE: break
			go WriteParquet(outputParquet, writerChannel, failedWriterFlag, writerDone, logger)

			ReadWarc(recordsReader, writerChannel, failedWriterFlag, logger)

			// The reader ended, the file if completely processed and we can
			// inform the writer by closing the channel
			close(writerChannel)

			// Wait for the writer to complete
			<-writerDone

		}

		recordsReader.Close()
		fileReader.Close()

	}
}


func ReadWarc(recordsReader *warc.Reader, writersChannel chan *PagesList,
	failedWriterFlag *abool.AtomicBool, logger Logger) {
	//first:=0
	pagesBuffer := PagesList{}
	for {
		if pagesBuffer.length >= CHUNK_SIZE {

			// If the writer is dead, stop the reader
			if failedWriterFlag.IsSet() {
				//LOG FAILED
				logger.Exceptions <- Exception{
					ErrorType: "Reader controlled failure",
					Message:   "The writer failed and the reader is interrupting the job",
				}
				break
			}

			// Send the chunk and allocate a new list
			copied := pagesBuffer.copy()
			writersChannel <- &copied
			pagesBuffer = PagesList{}
		}

		record, err := recordsReader.ReadRecord()
		if err != nil {
			if err != io.EOF {
				logger.Exceptions <- Exception{
					ErrorType:       "Record malformed",
					Message:         "The reader failed to process the record",
					OriginalMessage: err.Error(),
				}
				panic(err)
			} else {
				break
			}
		} else {

			warcContentType := record.Header.Get("content-type")
			recordType := record.Header.Get("warc-type")

			if recordType == "response" && strings.HasPrefix(warcContentType, "application/http") {

				originalUrl := record.Header.Get("WARC-Target-URI")
				originalUrl = sanitizeString(originalUrl)

				//b,_:=io.ReadAll(record.Content)
				//if first<57{
				//	fmt.Println(string(b))
				//	fmt.Println(originalUrl)
				//	first=first+1
				//}





				pageUrl, err := url.Parse(originalUrl)
				//fmt.Println(pageUrl)
				//



				if err != nil {
					logger.Exceptions <- Exception{
						ErrorType:       "Not an URL",
						Message:         originalUrl,
						OriginalMessage: err.Error(),
					}
				} else if !isWikimediaProject(pageUrl.Host){



					reader := bufio.NewReader(record.Content)
					var httpStatusCode string
					//var redirectLocation string
					var contentType string

					for {
						lineBytes, _, err := reader.ReadLine()

						if err == io.EOF {
							break
						}
						if len(lineBytes) < 1 {
							break
						}

						line := string(lineBytes)

						if strings.HasPrefix(line, "HTTP/") {
							if len(line) >= 12 {
								httpStatusCode = line[9:12]
							}
						}

						//if strings.HasPrefix(line, "Location:") {
						//	if len(line) >= 10 {
						//		redirectLocation = line[10:]
						//	}
						//}

						if strings.HasPrefix(line, "Content-Type:") {
							if len(line) >= 14 {
								contentType = line[14:]
							}
						}

					}

					//extras := ""
					if httpStatusCode == "200" {

						if strings.HasPrefix(contentType, "text/html") {



							//https://stackoverflow.com/questions/39791021/how-to-read-multiple-times-from-same-io-reader
							customReader := getCharsetReader(reader, contentType)


							var buf bytes.Buffer
							tee := io.TeeReader(customReader, &buf)



							if hasWikimediaLinks(tee) {
								bytes, err:=ioutil.ReadAll(&buf)
								if err == nil {
									normalizedPageUrl := purell.NormalizeURL(pageUrl, PURELL_FLAGS)
									pageContent := NewPage(normalizedPageUrl, string(bytes))
									pagesBuffer.append(&pageContent)
								}

							}



							}
						}

					}


				}

			}

		}

	writersChannel <- &pagesBuffer

	}



func hasWikimediaLinks(body io.Reader) bool {

	//Initialise tokenizer
	tokenizer := html.NewTokenizer(body)
	hasLinks := false
	for {
		//get the next token type
		tokenType := tokenizer.Next()

		if tokenType == html.StartTagToken || tokenType == html.SelfClosingTagToken {
			//Get token info
			token := tokenizer.Token()

			// Tag a
			if "a" == token.Data {
				var hrefValue string
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						hrefValue = sanitizeString(attr.Val)
						break
					}
				}
				//fmt.Println(hrefValue)
				if !strings.HasPrefix(hrefValue, "javascript:") &&
					!strings.HasPrefix(hrefValue, "#") {

					//normalizedHrefValue,_ := getAbsoluteNormalized(pageUrl, hrefValue)

					//fmt.Println("\t"+pageUrl.Host)

					//if strings.Contains(pageUrl.Host,"blogspot") {
					//	fmt.Println(pageUrl.Host)
					//	fmt.Println(isWikimediaProjectFromURL(hrefValue))
					//}
					//if strings.Contains(pageUrl.Host,"03fotos") {
						if len(hrefValue) > 0 && isWikimediaProjectFromURL(hrefValue) {
							hasLinks=true
						}
					//}

				}

			}

		} else if tokenType == html.ErrorToken {
			err := tokenizer.Err()
			if err == io.EOF {
				//end of the file, break out of the loop
				break
			}
		}

	}

	return hasLinks
}



func WriteParquet(destination string, writersChannel chan *PagesList, failed *abool.AtomicBool, done chan bool, logger Logger) {

	//fmt.Println("Write in", destination)
	fw, err := local.NewLocalFileWriter(destination)
	if err != nil {
		failed.Set()
		logger.Exceptions <- Exception{
			//Source:          exceptionsSource,
			ErrorType:       "Write failed",
			Message:         "Impossible to create the file",
			OriginalMessage: err.Error(),
		}
		panic(err)
		// LOG IMPOSSIBLE TO CREATE THE FILE
	} else {
		//write
		pw, err := writer.NewParquetWriter(fw, new(Page), 1)
		if err != nil {
			failed.Set()
			// LOG IMPOSSIBLE TO CREATE THE FILE
			panic(err)
		} else {

			pw.RowGroupSize = 8 * 1024 * 1024
			pw.CompressionType = parquet.CompressionCodec_GZIP
			pw.PageSize = 2 * 1024 * 1024

			// Iterate until it is open
			for linksChunk := range writersChannel {
				fmt.Println("New write request:", linksChunk.length, "pages")
				for node := linksChunk.head; node != nil; node = node.next {
					if err := pw.Write(node.Marker); err != nil {
						failed.Set()
						logger.Exceptions <- Exception{
							//Source:          exceptionsSource,
							ErrorType:       "Write failed",
							Message:         "Impossible to write the record",
							OriginalMessage: err.Error(),
						}
						//LOG ERROR IN WRITING
						panic(err)

					}
				}
			}

			if err := pw.WriteStop(); err != nil {
				failed.Set()
				logger.Exceptions <- Exception{
					//Source:          exceptionsSource,
					ErrorType:       "Write failed",
					Message:         "Impossible to finalize the file",
					OriginalMessage: err.Error(),
				}
				// LOG IMPOSSIBLE TO FINALISE THE FILE
				panic(err)
			}
			fw.Close()
		}

	}

	done <- true
}
