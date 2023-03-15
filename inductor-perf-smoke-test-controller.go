package main

import (
	"fmt"
	"log"
	"time"

	"errors"
	"flag"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"github.com/rockset/rockset-go-client"
	//"os/exec"
	//"github.com/estebangarcia21/subprocess"
	"gopkg.in/go-rillas/subprocess.v1"

)

//type JenkinsQueue struct {
//	Items []struct {
//		Buildable bool   `json:"buildable"`
//		Why       string `json:"why"`
//		Task      struct {
//			Name string `json:"name"`
//		} `json:"task"`
//	} `json:"items"`
//}

//type JenkinsJob struct {
//	Color           string `json:"color"`
//	NextBuildNumber int    `json:"nextBuildNumber"`
//}

type GHARunnerInfo struct {
	Idle               bool `json:"idle"`
	TemporarilyOffline bool `json:"temporarilyOffline"`
	Offline            bool `json:"offline"`
	MonitorData        struct {
		HudsonNodeMonitorsArchitectureMonitor *string `json:"hudson.node_monitors.ArchitectureMonitor"`
	} `json:"monitorData"`
}

var gceProjectName *string
var gceZone *string
var locationName *string
var workersPerBuildBox *int
var jobNameRequiringAllNodes *string
var preferredNodeToKeepOnline *string

var buildBoxesPool = []string{}
var httpClient = &http.Client{}
var service *compute.Service

var lastSeenBuildNumber int

var lastStarted = struct {
	sync.RWMutex
	m map[string]time.Time
}{m: make(map[string]time.Time)}

func main() {
	defer func() {
		if e := recover(); e != nil {
			log.Printf("\n\033[31;1m%s\x1b[0m\n", e)
			os.Exit(1)
		}
	}()

	workersPerBuildBox = flag.Int("workersPerBuildBox", 1, "number of workers per build box")
	localCreds := flag.Bool("useLocalCreds", true, "uses the local creds.json as credentials for Google Cloud APIs")
	jobType := flag.String("jobType", "auto_scaling", "defines which job to execute: auto_scaling, all_up, all_down")
	gceProjectName = flag.String("gceProjectName", "", "project name where nodes are setup in GCE")
	gceZone = flag.String("gceZone", "us-central1-a", "GCE zone where nodes have been setup")
	locationName = flag.String("locationName", "US/Iowa", "Location used to determine working hours")
	jobNameRequiringAllNodes = flag.String("jobNameRequiringAllNodes", "", "job name which requires all build nodes enabled")
	preferredNodeToKeepOnline = flag.String("preferredNodeToKeepOnline", "", "name of the node that should be kept online")
	flag.Parse()

	validateFlags()

	if len(flag.Args()) == 0 {
		log.Println("At least one node name has to be specified")
		os.Exit(1)
	}

	buildBoxesPool = flag.Args()

	var err error
	if *localCreds {
		service, err = getServiceWithCredsFile()
	} else {
		service, err = getServiceWithDefaultCreds()
	}
	if err != nil {
		log.Printf("Error getting creds: %s\n", err.Error())
		return
	}

	switch *jobType {
	case "all_up":
		enableAllBuildBoxes()
	case "all_down":
		disableAllBuildBoxes()
	default:
		autoScaling()
	}
}

func validateFlags() {
	valid := true
	if *gceProjectName == "" {
		log.Println("gceProjectName flag should not be empty")
		valid = false
	}

	if !valid {
		os.Exit(1)
	}
}

func autoScaling() {
	for {
		queueSize := fetchQueueSize()
		//queueSize = adjustQueueSizeDependingWhetherJobRequiringAllNodesIsRunning(queueSize)

		if queueSize > 0 {
			log.Printf("%d jobs waiting to be executed\n", queueSize)
			enableMoreNodes(queueSize)
		} else if queueSize == 0 {
			log.Println("No jobs in the queue")
			disableUnnecessaryBuildBoxes()
		}

		log.Println("Iteration finished")
		fmt.Println("")
		time.Sleep(time.Second * 120)
	}
}

func enableMoreNodes(queueSize int) {
	boxesNeeded := calculateNumberOfNodesToEnable(queueSize)
	log.Printf("%d runners needed", boxesNeeded)
	log.Println("Checking if any runner is offline")
	var wg sync.WaitGroup
	buildBoxesPool = shuffle(buildBoxesPool)
	log.Println(buildBoxesPool)
	for _, buildBox := range buildBoxesPool {
		log.Println(buildBox)
		//if isNodeOffline(buildBox) 
		{
			wg.Add(1)
			go func(b string) {
				defer wg.Done()
				enableNode(b)
			}(buildBox)
			boxesNeeded = boxesNeeded - 1
			log.Printf("%d more runners needed\n", boxesNeeded)
		}
		if boxesNeeded <= 0 {
			wg.Wait()
			return
		}
	}
	wg.Wait()
	log.Println("No more runners available to start")
}

func shuffle(slice []string) []string {
	for i := range slice {
		randomInt := rand.Intn(i + 1)
		first := slice[i]
		second := slice[randomInt]
		slice[randomInt] = first
		slice[i] = second
	}
	return slice
}

func enableNode(buildBox string) bool {
	log.Printf("%s may be offline, trying to toggle it online\n", buildBox)
	startCloudBox(buildBox)
	return true
}

func startCloudBox(buildBox string) {
	if isCloudBoxRunning(buildBox) {
	  log.Printf("%s is already online, no op needed\n", buildBox)
		return
	}

	_, err := service.Instances.Start(*gceProjectName, *gceZone, buildBox).Do()
	if err != nil {
		log.Println(err)
		return
	}
	waitForStatus(buildBox, "RUNNING")
	lastStarted.Lock()
	lastStarted.m[buildBox] = time.Now()
	lastStarted.Unlock()
}

func calculateNumberOfNodesToEnable(queueSize int) int {
	mod := 0
	if queueSize%(*workersPerBuildBox) != 0 {
		mod = 1
	}

	return (queueSize / *workersPerBuildBox) + mod
}

func disableUnnecessaryBuildBoxes() {
	//var buildBoxToKeepOnline string
	other := "box"
	//if isWorkingHour() {
	//	buildBoxToKeepOnline = keepOneBoxOnline()
	//	other = "other runner apart from " + buildBoxToKeepOnline
	//}

	log.Printf("Checking if any %s is enabled and idle", other)
	var wg sync.WaitGroup
	for _, buildBox := range buildBoxesPool {
		//if buildBoxToKeepOnline != buildBox {
			wg.Add(1)
			go func(b string) {
				defer wg.Done()
				disableNode(b)
			}(buildBox)
		//}
	}
	wg.Wait()
}

//func keepOneBoxOnline() string {
//	preferredBoxPresent := false
//	for _, buildBox := range buildBoxesPool {
//		if buildBox == *preferredNodeToKeepOnline {
//			preferredBoxPresent = true
//			break
//		}
//	}
//
//	var buildBoxToKeepOnline string
//	if preferredBoxPresent && isCloudBoxRunning(*preferredNodeToKeepOnline) && !isNodeOffline(*preferredNodeToKeepOnline) && !isNodeTemporarilyOffline(*preferredNodeToKeepOnline) {
//		buildBoxToKeepOnline = *preferredNodeToKeepOnline
//	} else if preferredBoxPresent {
//		if enableNode(*preferredNodeToKeepOnline) {
//			buildBoxToKeepOnline = *preferredNodeToKeepOnline
//		}
//	}
//
//	if buildBoxToKeepOnline == "" {
//		online := make(chan string, len(buildBoxesPool))
//		for _, buildBox := range buildBoxesPool {
//			go func(b string, channel chan<- string) {
//				if isCloudBoxRunning(b) && !isNodeOffline(b) && !isNodeTemporarilyOffline(b) {
//					channel <- b
//					return
//				}
//				channel <- ""
//			}(buildBox, online)
//		}
//
//		for range buildBoxesPool {
//			b := <-online
//			if b != "" {
//				buildBoxToKeepOnline = b
//				log.Printf("Will keep %s online", b)
//				break
//			}
//		}
//	}
//
//	if buildBoxToKeepOnline == "" {
//		buildBoxToKeepOnline = shuffle(buildBoxesPool)[0]
//		log.Printf("Will start %s and keep online", buildBoxToKeepOnline)
//		enableNode(buildBoxToKeepOnline)
//	}
//
//	return buildBoxToKeepOnline
//}

func isWorkingHour() bool {
	location, err := time.LoadLocation(*locationName)
	if err != nil {
		fmt.Printf("Could not load %s location\n", *locationName)
		return true
	}

	t := time.Now().In(location)
	if t.Hour() < 7 || t.Hour() > 19 {
		log.Println("Nobody should be working at this time of the day...")
		return false
	}
	if t.Weekday() == 0 || t.Weekday() == 6 {
		log.Println("Nobody should be working on weekends...")
		return false
	}
	return true
}

func disableNode(buildBox string) {
	if !isNodeIdle(buildBox) {
		return
	}

	lastStarted.RLock()
	started := lastStarted.m[buildBox]
	lastStarted.RUnlock()
	if !started.IsZero() && started.Add(time.Minute*10).After(time.Now()) {
		log.Printf("%s is idle but has been up for less than 10 minutes", buildBox)
		return
	}

	if !isNodeTemporarilyOffline(buildBox) {

		time.Sleep(2 * time.Second)
		if !isNodeIdle(buildBox) {
			log.Printf("%s accepted a new job in the meantime, aborting termination\n", buildBox)
			return
		}
	}

	ensureCloudBoxIsNotRunning(buildBox)
}

func stopCloudBox(buildBox string) error {
	_, err := service.Instances.Stop(*gceProjectName, *gceZone, buildBox).Do()
	if err != nil {
		log.Println(err)
		return err
	}
	waitForStatus(buildBox, "TERMINATED")

	lastStarted.Lock()
	lastStarted.m[buildBox] = time.Time{}
	lastStarted.Unlock()
	return nil
}

func isNodeOffline(buildBox string) bool {
	data := fetchNodeInfo(buildBox)

	return data.Offline
}

func isNodeTemporarilyOffline(buildBox string) bool {
	data := fetchNodeInfo(buildBox)

	return data.TemporarilyOffline
}

func isNodeIdle(runner string) bool {
	// Commenting out fetchNode from Rockset 
	// Rely on runner info entirely
	//data := fetchNodeInfoFromRockset(buildBox)
	var data GHARunnerInfo
	log.Println("in isNodeIdle")
	//s := subprocess.New("/data/home/weiwangmeta/tools/google-cloud-sdk/bin/gcloud compute ssh gh-ci-gcp-a100-4 -- \"cat /tmp/runner_status \"", subprocess.Shell)
        //if err := s.Exec(); err != nil {
        //  log.Fatal(err)
        //}
	//fmt.Printf("The output is %s\n", s.Stdout())
	var cmd_part1 string
	cmd_part1 = "/data/home/weiwangmeta/tools/google-cloud-sdk/bin/gcloud compute ssh "
	var cmd_part2 string
	cmd_part2 = " -- \" cat /tmp/runner_status \""
	cmd_text := cmd_part1 + runner + cmd_part2
	if isCloudBoxRunning(runner) {
	    response := subprocess.RunShell("", "", cmd_text)
            // print the standard output stream data
	    fmt.Printf("Out: Runner %s status %s\n", runner, response.StdOut)
            // print the standard error stream data
            //fmt.Printf("Error %s\n", response.StdErr)
            // print the exit status code integer value
            //fmt.Printf("ExitCode %d\n", response.ExitCode)
	    // If data.Idle is true, meaning rockset thinks the runner is idle
	    // Do not trust rockset and trust response instead
	    // Otherwise if data.Idle is 
            if strings.Contains(response.StdOut, "busy") {
	      fmt.Printf("Response received, setting Idle to false\n")
	      data.Idle = false
	    } else {
	      fmt.Printf("Response does not seem to be busy, setting Idle to true for runner %s\n", runner)
	      data.Idle = true
	    }
        } else {
	    fmt.Printf("Runner %s is not running, therefore data.Idle can be false (busy), no action needed\n", runner)
	    data.Idle = false
	}
	return data.Idle
}

func ExampleRockClient_query(runner string) GHARunnerInfo {
	ctx := context.TODO()

	rc, err := rockset.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	var data GHARunnerInfo
	fmt.Printf("Current runner is: %s\n", runner)
	var query_part1 string
	query_part1  = "select job._event_time,    w.name as workflowName,    job.html_url as htmlUrl,    job.runner_name,    job.status from    commons.workflow_job job    join commons.workflow_run w on w.id = job.run_id where    job.runner_name like '"
	var query_part2 string
	query_part2 = "' order by    job._event_time DESC limit    1"
	query_text := query_part1 + runner + query_part2
	fmt.Printf("SQL Query is: %s\n", query_text)
	r, err := rc.Query(ctx, query_text)
//		option.WithWarnings(), option.WithRowLimit(10),
//		option.WithParameter("label", "string", "QUERY_SUCCESS"))
	if err != nil {
		log.Fatal(err)
	}

	for _, c := range r.Results {
		fmt.Printf("result: %s\n", c)
                if (c["status"] == "completed") {
                  //fmt.Printf("results: %f\n", c["count"])
                  data.Idle = true 
                  fmt.Printf("Runner %s Idle status is %v\n", runner, data.Idle)
		}
	}
	return data
}

func fetchNodeInfoFromRockset(runner string) GHARunnerInfo {
        return ExampleRockClient_query(runner)
}

func fetchNodeInfo(buildBox string) GHARunnerInfo {
	return GHARunnerInfo{}

	var data GHARunnerInfo

	return data
}

func adjustQueueSizeDependingWhetherJobRequiringAllNodesIsRunning(queueSize int) int {
	return queueSize
}

func get_qsize_RockClient_queryLambda() int {
	ctx := context.TODO()

	rc, err := rockset.NewClient()
	if err != nil {
		log.Fatal(err)
	}

	r, err := rc.ExecuteQueryLambda(ctx, "metrics", "queued_jobs_by_label" )
	if err != nil {
		log.Fatal(err)
	}
	var qsize int = 0
	for _, c := range r.Results {
		fmt.Printf("machine type: %s\n", c["machine_type"])
		if (c["machine_type"] == "linux.gcp.a100") {
		  //fmt.Printf("results: %f\n", c["count"])
		  qsize = int(c["count"].(float64))
		  fmt.Printf("queue size (int) is : %v\n", qsize)
	        }
	}

	return qsize
}



func fetchQueueSize() int {
	return get_qsize_RockClient_queryLambda()
}

func ensureCloudBoxIsNotRunning(buildBox string) {
	if isCloudBoxRunning(buildBox) {
		log.Printf("%s is running... Stopping\n", buildBox)
		stopCloudBox(buildBox)
	}
}

func isCloudBoxRunning(buildBox string) bool {
	i, err := service.Instances.Get(*gceProjectName, *gceZone, buildBox).Do()
	if nil != err {
		log.Printf("Failed to get instance data: %v\n", err)
		return false
	}
	log.Println(buildBox)
	log.Println(i.Status)
	return i.Status == "RUNNING"
}

func enableAllBuildBoxes() {
	log.Println("Spinning up all build runners specified")
	var wg sync.WaitGroup
	for _, buildBox := range buildBoxesPool {
		if isNodeOffline(buildBox) {
			wg.Add(1)
			go func(b string) {
				defer wg.Done()
				enableNode(b)
			}(buildBox)
		}
	}
	wg.Wait()
}

func disableAllBuildBoxes() {
	log.Println("Terminating all build runners specified")
	var wg sync.WaitGroup
	for _, buildBox := range buildBoxesPool {
		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			ensureCloudBoxIsNotRunning(b)
		}(buildBox)
	}
	wg.Wait()
}

func waitForStatus(buildBox string, status string) {
	completed := make(chan bool, 1)
	quit := make(chan bool)

	go func() {
		previousStatus := ""
		for {
			select {
			case <- quit:
				return
			default:
			}

			i, err := service.Instances.Get(*gceProjectName, *gceZone, buildBox).Do()
			if nil != err {
				log.Printf("Failed to get instance data for %s: %v\n", buildBox, err)
				continue
			}

			if previousStatus != i.Status {
				log.Printf("    %s -> %s\n", buildBox, i.Status)
				previousStatus = i.Status
			}

			if i.Status == status {
				log.Printf("    %s reached %s status\n", buildBox, status)
				break
			}

			time.Sleep(time.Second * 3)
		}

		completed <- true
	}()

	select {
	case <-completed:
	case <-time.After(1 * time.Minute):
		quit <- true
		log.Printf("    %s did not reach %s status within a reasonable time\n", buildBox, status)
	}
}

func getServiceWithCredsFile() (*compute.Service, error) {
	optionAPIKey := option.WithServiceAccountFile("creds.json")
	if optionAPIKey == nil {
		log.Println("Error creating option.WithAPIKey")
		return nil, errors.New("Error creating option.WithAPIKey")
	}
	optScope := []option.ClientOption{
		option.WithScopes(compute.ComputeScope),
	}
	optionSlice := append(optScope, optionAPIKey)
	ctx := context.TODO()

	httpClient, _, err := transport.NewHTTPClient(ctx, optionSlice...)
	if err != nil {
		log.Printf("Error NewHTTPClient: %s\n", err.Error())
		return nil, err
	}

	service, err := compute.New(httpClient)
	if err != nil {
		log.Printf("Error compute.New(): %s\n", err.Error())
		return nil, err
	}
	return service, nil
}

func getServiceWithDefaultCreds() (*compute.Service, error) {
	ctx := context.TODO()

	client, err := google.DefaultClient(ctx, compute.ComputeScope)
	if err != nil {
		return nil, err
	}
	computeService, err := compute.New(client)
	return computeService, err
}
