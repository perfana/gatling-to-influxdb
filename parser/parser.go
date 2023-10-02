/*
Copyright © 2020 Anton Kramarev

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package parser

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"
	"strings"

	"github.com/dakaraj/gatling-to-influxdb/influx"
	l "github.com/dakaraj/gatling-to-influxdb/logger"

	// infc "github.com/influxdata/influxdb1-client/v2"
	"github.com/spf13/cobra"
)

const (
	oneMillisecond        = 1_000_000
	simulationLogFileName = "simulation.log"
	// Constant amounts of elements per log line
	runLineLen     = 6
	requestLineLen = 8
	groupLineLen   = 7
	userLineLen    = 6
	errorLineLen   = 3
)

var (
	resultDirNamePattern = regexp.MustCompile(`^.+?-(\d{14})\d{3}$`)
	startTime            = time.Now().Unix()
	nodeName             string
	gatlingVersion       string

	errFound         = errors.New("Found")
	errStoppedByUser = errors.New("Process stopped by user")
	errFatal         = errors.New("Fatal error")
	logDir           string
	systemUnderTest  string
	testEnvironment  string
	simulationName   string
	waitTime         uint

	tabSep = []byte{9}

	// regular expression patterns for matching log strings
	userLine    = regexp.MustCompile(`^USER\s`)
	requestLine = regexp.MustCompile(`^REQUEST\s`)
	groupLine   = regexp.MustCompile(`GROUP\s`)
	runLine     = regexp.MustCompile(`^RUN\s`)
	errorLine   = regexp.MustCompile(`^ERROR\s`)

    http_reqs         = regexp.MustCompile(`^http_reqs\s`)
    http_req_duration = regexp.MustCompile(`^http_req_duration\s`)
    grpc_req_duration = regexp.MustCompile(`^grpc_req_duration\s`)
    group_duration    = regexp.MustCompile(`^group_duration\s`)


	parserStopped = make(chan struct{})
)

func lookupTargetDir(ctx context.Context, dir string) error {
	const loopTimeout = 5 * time.Second

	l.Infoln("Looking for target directory...")
	for {
		// This block checks if stop signal is received from user
		// and stops further lookup
		select {
		case <-ctx.Done():
			return errStoppedByUser
		default:
		}

		fInfo, err := os.Stat(dir)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("Target path %s exists but there is an error: %w", dir, err)
		}
		if os.IsNotExist(err) {
			time.Sleep(loopTimeout)
			continue
		}

		if !fInfo.IsDir() {
			return fmt.Errorf("Was expecting directory at %s, but found a file", dir)
		}

		abs, _ := filepath.Abs(dir)
		l.Infof("Target directory found at %s", abs)
		break
	}

	return nil
}

func walkFunc(path string, info os.FileInfo, err error) error {
	if info.IsDir() && resultDirNamePattern.MatchString(info.Name()) {
		dateString := resultDirNamePattern.FindStringSubmatch(info.Name())[1]
		t, _ := time.Parse("20060102150405", dateString)
		if t.Unix() > startTime {
			logDir = path
			l.Infof("Found log directory at %s", logDir)

			return errFound
		}
	}

	return nil
}

// logic is the following: at the start of the application current timestamp is saved
// then traversing over all directories inside target dir is initiated.
// Every dir name is matched against pattern, if found - date time from dir name
// is parsed and  result timestamp is matched against application start time.
// Function stops as soon as matched date time is higher then initial one
func lookupResultsDir(ctx context.Context, dir string) error {
	const loopTimeout = 5 * time.Second

	l.Infof("Searching for results directory...")
	for {
		// This block checks if stop signal is received from user
		// and stops further lookup
		select {
		case <-ctx.Done():
			return errStoppedByUser
		default:
		}

		err := filepath.Walk(dir, walkFunc)
		if err == errFound {
			break
		}
		if err != nil {
			return err
		}

		time.Sleep(loopTimeout)
	}

	return nil
}

func waitForLog(ctx context.Context) error {
	const loopTimeout = 5 * time.Second

	l.Infoln("Searching for " + simulationLogFileName + " file...")
	for {
		// This block checks if stop signal is received from user
		// and stops further lookup
		select {
		case <-ctx.Done():
			return errStoppedByUser
		default:
		}

		fInfo, err := os.Stat(logDir + "/" + simulationLogFileName)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if os.IsNotExist(err) {
			time.Sleep(loopTimeout)
			continue
		}

		// WARNING: second part of this check may fail on Windows. Not tested
		if fInfo.Mode().IsRegular() && (runtime.GOOS == "windows" || fInfo.Mode().Perm() == 420) {
			abs, _ := filepath.Abs(logDir + "/" + simulationLogFileName)
			l.Infof("Found %s\n", abs)
			break
		}

		return errors.New("Something wrong happened when attempting to open " + simulationLogFileName)
	}

	return nil
}

func timeFromUnixBytes(ub []byte) (time.Time, error) {
	timeStamp, err := strconv.ParseInt(string(ub), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("Failed to parse timestamp as integer: %w", err)
	}
	// A workaround that adds random amount of microseconds to the timestamp
	// so db entries will (should) not be overwritten
	return time.Unix(0, timeStamp*oneMillisecond+rand.Int63n(oneMillisecond)), nil
}

func http_req_duration_LineProcess(lb []byte) error {


split := bytes.Split(lb, []byte(","))
		if len(split) != 18 {
			return errors.New("Line contains unexpected amount of values")
		}

		timestamp, err := timeFromUnixBytes(split[1])
		if err != nil {
			return err
		}

		duration, err := strconv.Atoi(string(split[2]))
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}

        requestPoint, err := influx.NewPoint(
        "http_req_duration",
        map[string]string{
            "name": strings.TrimSpace(strings.ReplaceAll(string(split[9]), " ", "_")),
            "group": strings.TrimSpace(strings.ReplaceAll(string(split[7]), " ", "_")),
            "method": string(split[8]),
            "expected_response": string(split[6]),
            "systemUnderTest": systemUnderTest,
            "testEnvironment": testEnvironment,
            "nodeName":   nodeName,
            "status": string(split[13]),
            "service": string(split[12]),
            "url": string(split[16]),
            "scenario": string(split[11]),
            "error_code": string(split[5]),
            "error": string(bytes.TrimSpace(split[4])),
        },
        map[string]interface{}{
            "duration":  duration   ,
        },
            timestamp,
			)
			if err != nil {
				return fmt.Errorf("Error creating new point with request data: %w", err)
			}

			influx.SendPoint(requestPoint)

	return nil
}
func grpc_req_duration_LineProcess(lb []byte) error {


split := bytes.Split(lb, []byte(","))
		if len(split) != 18 {
			return errors.New("Line contains unexpected amount of values")
		}

		timestamp, err := timeFromUnixBytes(split[1])
		if err != nil {
			return err
		}

		duration, err := strconv.Atoi(string(split[2]))
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}

        requestPoint, err := influx.NewPoint(
        "grpc_req_duration",
        map[string]string{
            "name": strings.TrimSpace(strings.ReplaceAll(string(split[9]), " ", "_")),
            "group": strings.TrimSpace(strings.ReplaceAll(string(split[7]), " ", "_")),
            "method": string(split[8]),
            "expected_response": string(split[6]),
            "systemUnderTest": systemUnderTest,
            "testEnvironment": testEnvironment,
            "nodeName":   nodeName,
            "status": string(split[13]),
            "service": string(split[12]),
            "url": string(split[16]),
            "scenario": string(split[11]),
            "error_code": string(split[5]),
            "error": string(bytes.TrimSpace(split[4])),
        },
        map[string]interface{}{
            "duration":  duration   ,
        },
            timestamp,
			)
			if err != nil {
				return fmt.Errorf("Error creating new point with request data: %w", err)
			}

			influx.SendPoint(requestPoint)

	return nil
}

func group_duration_LineProcess(lb []byte) error {


split := bytes.Split(lb, []byte(","))
		if len(split) != 18 {
			return errors.New("Line contains unexpected amount of values")
		}

		timestamp, err := timeFromUnixBytes(split[1])
		if err != nil {
			return err
		}

		duration, err := strconv.Atoi(string(split[2]))
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}

        requestPoint, err := influx.NewPoint(
        "group_duration",
        map[string]string{
            "group": strings.TrimSpace(strings.ReplaceAll(string(split[7]), " ", "_")),
            "scenario": string(split[11]),

        },
        map[string]interface{}{
            "duration":  duration   ,
        },
            timestamp,
			)
			if err != nil {
				return fmt.Errorf("Error creating new point with request data: %w", err)
			}

			influx.SendPoint(requestPoint)

	return nil
}


func stringProcessor(lineBuffer []byte, gatlingVersion string) error {


	switch {
	case http_req_duration.Match(lineBuffer):
		return http_req_duration_LineProcess(lineBuffer)
	case grpc_req_duration.Match(lineBuffer):
		return grpc_req_duration_LineProcess(lineBuffer)
	case group_duration.Match(lineBuffer):
		return group_duration_LineProcess(lineBuffer)
	default:
		return fmt.Errorf("Unknown line type encountered")
	}
}

func fileProcessor(ctx context.Context, file *os.File) {
	r := bufio.NewReader(file)
	buf := new(bytes.Buffer)
	startWait := time.Now()

ParseLoop:
	for {
		// This block checks if stop signal is received from user
		// and stops further processing
		select {
		case <-ctx.Done():
			l.Infoln("Parser received closing signal. Processing stopped")
			break ParseLoop
		default:
		}

		b, err := r.ReadBytes('\n')
		if err == io.EOF {
			// If no new lines read for more than value provided by 'stop-timeout' key then processing is stopped
			if time.Now().After(startWait.Add(time.Duration(waitTime) * time.Second)) {
				l.Infof("No new lines found for %d seconds. Stopping application...", waitTime)
				break ParseLoop
			}
			// All new data is stored in buffer until next loop
			buf.Write(b)
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			l.Errorf("Unexpected error encountered while parsing file: %v", err)
		}

		buf.Write(b)
		err = stringProcessor(buf.Bytes(), gatlingVersion)
		if err != nil {
			l.Errorf("String processing failed: %v", err)
			if errors.Is(err, errFatal) {
				l.Errorln("Log parser caught an error that can't be handled. Stopping application...")
				break ParseLoop
			}
		}
		// Clean buffer after processing preparing for a new loop
		buf.Reset()
		// Reset a timeout timer
		startWait = time.Now()
	}
	parserStopped <- struct{}{}
}

func parseStart(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	l.Infoln("Starting log file parser...")
	file, err := os.Open(logDir + "/" + simulationLogFileName)
	if err != nil {
		l.Errorf("Failed to read %s file: %v\n", simulationLogFileName, err)
	}
	defer file.Close()

	fileProcessor(ctx, file)
}

// RunMain performs main application logic
func RunMain(cmd *cobra.Command, dir string) {
	systemUnderTest, _ = cmd.Flags().GetString("system-under-test")
	testEnvironment, _ = cmd.Flags().GetString("test-environment")
	waitTime, _ = cmd.Flags().GetUint("stop-timeout")
	rand.Seed(time.Now().UnixNano())
	nodeName, _ = os.Hostname()

	l.Infof("Searching for directory at %s", dir)
	abs, err := filepath.Abs(dir)
	if err != nil {
		l.Errorf("Failed to construct an absolute path for %s: %v", dir, err)
	}

	if err := lookupTargetDir(cmd.Context(), abs); err != nil {
		if err == errStoppedByUser {
			return
		}
		l.Errorf("Target directory lookup failed with error: %v\n", err)
		os.Exit(1)
	}

	if err := lookupResultsDir(cmd.Context(), abs); err != nil {
		if err == errStoppedByUser {
			return
		}
		l.Errorf("Error happened while searching for results directory: %v\n", err)
		os.Exit(1)
	}

	if err := waitForLog(cmd.Context()); err != nil {
		if err == errStoppedByUser {
			return
		}
		l.Errorf("Failed waiting for %s with error: %v\n", simulationLogFileName, err)
		os.Exit(1)
	}

	wg := &sync.WaitGroup{}
	pCtx, pCancel := context.WithCancel(context.Background())
	iCtx, iCancel := context.WithCancel(context.Background())

	wg.Add(2)
	go parseStart(pCtx, wg)
	go influx.StartProcessing(iCtx, wg)

FinisherLoop:
	for {
		select {
		// If top level context is cancelled we first stop the parser
		case <-cmd.Context().Done():
			pCancel()
		// Then wait for parser to stop and stop client processing
		case <-parserStopped:
			iCancel()
			// In case parser finished processing on its own, we cancel its context
			pCancel()
			break FinisherLoop
		}
	}
	wg.Wait()
}
