package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type revList []string

type ServiceRevision struct {
	Id          int64
	ServiceName string `xorm:"varchar(200) notnull 'service_name'"`
	RevId       string `xorm:"varchar(64) 'rev_id'"`
	CommitId	string `xorm:"varchar(16) 'commit_id'"`
	Deployed    int8   `xorm:"tinyint notnull 'deployed'"`
}

type ServiceTag struct {
	Id          int64
	ServiceName string    `xorm:"varchar(200) notnull 'service_name'"`
	Tag         string    `xorm:"varchar(50) notnull 'tag'"`
	Uat         int8      `xorm:"tinyint notnull 'uat'"`
	ProdBeta      int8      `xorm:"tinyint notnull 'prod_beta'"`
	ProdAlpha     int8      `xorm:"tinyint notnull 'prod_alpha'"`
	UpdatedAt   time.Time `xorm:"updated"`
}

type ReplicationStatus struct {
	Id            int
	Status        string
	Repository    string
	Policy_id     int
	Operation     string
	Tags          []string
	Creation_time string
	Update_time   string
}

var (
	svcMap = make(map[string]revList)
	svcDelMap = make(map[string]bool)

	changIdCommitIdMap = make(map[string]string)

	updateList = make([]string, 0)

	verbose = flag.Bool("v", false, "verbose mode")

	idLength = flag.Int("s", 7, "commit-id SHA1 length")

	revFilePath = flag.String("f", "", "Input revision list file")

	dataSource = flag.String("d", "root:passwd@tcp(%s:3306)/git_repo", "Mysql data source uri")

	dataHost = flag.String("h", "localhost", "Mysql data source host")

	initMode = flag.Bool("X", false, "Initial Mode (update to newest SHA1)")

	deployList = flag.String("D", "", "Deployed service list")

	harborSvcList = flag.String("H", "", "Harbor service list")

	imageTag = flag.String("T", "", "Service image tag")

	getSvcTag = flag.String("g", "", "get service tag (from alpha or beta)")

	writeSvcTag = flag.String("w", "", "write service tag (from alpha or beta)")

	excludeSvc = flag.String("E", "", "exclude service list")

	repliMap = make(map[string]ReplicationStatus)
)

func main() {

	flag.Parse()

	if len(os.Args) == 1 {
		fmt.Println("Usage: parse [Options]")
		flag.PrintDefaults()
		return
	}

	*dataSource = fmt.Sprintf(*dataSource, *dataHost)

	// Get Service and tag from the prod-quick environment
	if len(*getSvcTag) > 0 {
		getReplication(*getSvcTag)

		fmt.Print(getServiceTag(*getSvcTag))
		return
	}

	// Write back deployment result in the prod-quick environment
	if len(*writeSvcTag) > 0 {
		writeServiceTag(*writeSvcTag)
		return
	}

	//Deploy finished
	if len(*deployList) > 0 && len(*harborSvcList) > 0 && len(*imageTag) > 0 {

		revList := writeDeployStatus()

		writeServiceTagUat()

		fmt.Print(revList)
		return
	}

	// Parse recent revision data
	err := readRevList(*revFilePath)
	if err != nil {
		return
	}

	if *initMode {

		initSvcEntry()

		fmt.Println("Service entry initialization finished.")

	} else {

		resolveSvc()

		fmt.Println(strings.Join(updateList, ","))
	}
}

func readRevList(revListFilename string) (err error) {

	inputFile, inputError := os.Open(revListFilename)
	if inputError != nil {
		fmt.Println("Can't read revision list file: ", revListFilename)
		return inputError
	}
	defer inputFile.Close()

	inputReader := bufio.NewReader(inputFile)

	var currentId string
	var currentCommitId string
	var isHead bool
	for {
		inputString, readerError := inputReader.ReadString('\n')
		inputString = strings.TrimRight(inputString, "\n")

		if strings.Index(inputString, "@@@") == 0 {
			isHead = true

			currentCommitId = inputString[3:3+*idLength]
		}

		if strings.Index(inputString, "###") == 0 {
			isHead = false
			goto EXIT
		}

		if isHead {

			chIdx := strings.Index(inputString, "Change-Id: ")
			if chIdx != -1 {
				chIdx += 11
				currentId = inputString[chIdx:chIdx+41]

				changIdCommitIdMap[currentId] = currentCommitId
			}

		} else {

			strTokens := strings.Split(inputString, "\t")
			if len(strTokens) > 0 {

				sourcePart := strTokens[len(strTokens)-1]

				idx := strings.Index(sourcePart, "/src")

				//Part from ".../src"
				sourcePartType := 1

				if idx == -1 {
					idx = strings.Index(sourcePart, "/pom.xml")

					//Part from ".../pom.xml"
					sourcePartType = 2
				}

				if idx == -1 {
					idx = strings.Index(sourcePart, "/Dockerfile")

					//Part from ".../Dockerfile"
					sourcePartType = 3
				}

				if idx != -1 {
					svcName := sourcePart[:idx]

					if strTokens[0] == "D" {
						if sourcePartType == 2 {
							svcDelMap[svcName] = true  //The whole module will be removed
						}
						goto EXIT

					} else {
						if svcDelMap[svcName] {
							goto EXIT
						}
					}

					currRevList := svcMap[svcName]

					if currRevList == nil {
						currRevList = make(revList, 0)
					}

					if len(currRevList) > 0 {
						if currRevList[len(currRevList)-1] == currentId {
							goto EXIT
						}
					}

					currRevList = append(currRevList, currentId)

					svcMap[svcName] = currRevList

				}
			}
		}
	EXIT:
		if readerError == io.EOF {
			if *verbose {
				printSvcMap()
			}

			return nil
		}
	}
}

func printSvcMap() {

	for key, value := range svcMap {
		fmt.Printf("\n--- %s ---\n", key)

		for _, val := range value {
			fmt.Println(val)
		}

	}
}

func resolveSvc() {

	engine, _ := xorm.NewEngine("mysql", *dataSource)

	oldService := make([]ServiceRevision, 0)
	err := engine.Find(&oldService)
	if err != nil {
		panic("Query error occur")
	}

	for svc, svcRevList := range svcMap {

		if svcRevList != nil && len(svcRevList) > 0 {
			latestRevId := svcRevList[0]

			existed := false
			for _, oldSvc := range oldService {

				if svc == oldSvc.ServiceName { //Entry already exist
					if latestRevId != oldSvc.RevId {
						updateList = append(updateList, svc+"/pom.xml")

						oldSvc.RevId = latestRevId
						oldSvc.CommitId = changIdCommitIdMap[latestRevId]
						oldSvc.Deployed = 0
						_, _ = engine.ID(oldSvc.Id).Cols("rev_id", "commit_id", "deployed").Update(oldSvc)

					} else if oldSvc.Deployed == 0 {
						updateList = append(updateList, svc+"/pom.xml")
					}
					existed = true
					break
				}
			}

			if !existed { //New Entry
				//also need update
				updateList = append(updateList, svc+"/pom.xml")

				//Add Empty Entry
				serviceRevision := &ServiceRevision{ServiceName: svc, RevId: latestRevId, CommitId: changIdCommitIdMap[latestRevId]}
				_, _ = engine.Insert(serviceRevision)
			}
		}
	}
}

// Initialize service entry assuming that these services have already been deployed
func initSvcEntry() {

	engine, _ := xorm.NewEngine("mysql", *dataSource)

	for svc, svcRevList := range svcMap {
		if svcRevList != nil && len(svcRevList) > 0 {
			latestRevId := svcRevList[0]

			serviceRevision := &ServiceRevision{ServiceName: svc}
			has, _ := engine.Get(serviceRevision)

			serviceRevision.RevId = latestRevId
			serviceRevision.Deployed = 1

			if has {
				_, _ = engine.ID(serviceRevision.Id).Update(serviceRevision)

			} else {
				_, _ = engine.Insert(serviceRevision)
			}
		}
	}
}

// Write back every module's status to '1' while deploy is done
func writeDeployStatus() string {

	engine, _ := xorm.NewEngine("mysql", *dataSource)

	svcList := strings.Split(*deployList, ",")

	revList := make([]string, 0)

	for _, svc := range svcList {
		idx := strings.Index(svc, "/pom.xml")
		if idx != -1 {
			svcName := svc[:idx]

			serviceRevision := &ServiceRevision{ServiceName: svcName}
			has, _ := engine.Get(serviceRevision)
			if has {
				serviceRevision.Deployed = 1
				_, _ = engine.ID(serviceRevision.Id).Update(serviceRevision)

				revList = append(revList, serviceRevision.CommitId)
			}
		}
	}

	return strings.Join(revList, "|")
}

// Write down the service and tag which are actually deployed
func writeServiceTagUat() {

	engine, _ := xorm.NewEngine("mysql", *dataSource)

	svcList := strings.Split(*harborSvcList, " ")

	for _, svc := range svcList {

		serviceTag := &ServiceTag{ServiceName: svc}
		has, _ := engine.Get(serviceTag)

		serviceTag.Tag = *imageTag
		serviceTag.Uat = 1
		serviceTag.ProdBeta = 0
		serviceTag.ProdAlpha = 0
		if has {
			_, _ = engine.ID(serviceTag.Id).Cols("tag", "uat", "prod_beta", "prod_alpha").Update(serviceTag)
		} else {
			_, _ = engine.Insert(serviceTag)
		}

	}
}

func writeServiceTag(svcTag string) {
	engine, _ := xorm.NewEngine("mysql", *dataSource)

	svcTagToken := strings.Split(svcTag, ":")

	env := svcTagToken[0]
	svc := svcTagToken[1]
	tag := svcTagToken[2]

	serviceTag := &ServiceTag{ServiceName: svc, Tag: tag}
	has, _ := engine.Get(serviceTag)

	if has {
		if env == "alpha" {
			serviceTag.ProdAlpha = 1
		} else if env == "beta" {
			serviceTag.ProdBeta = 1
		}
		_, _ = engine.ID(serviceTag.Id).Update(serviceTag)
	}
}

func getServiceTag(env string) string {

	engine, _ := xorm.NewEngine("mysql", *dataSource)

	allTags := make([]ServiceTag, 0)

	if env == "alpha" {

		err := engine.Where("uat = 1 and prod_alpha = 0").Find(&allTags)
		if err != nil {
			panic("Query Tag error occur")
		}

	} else if env == "beta" {

		err := engine.Where("uat = 1 and prod_beta = 0").Find(&allTags)
		if err != nil {
			panic("Query Tag error occur")
		}
	}

	allSvcTags := make([]string, 0)
	allSvcTags = append(allSvcTags, "All")

	for _, val := range allTags {

		if len(*excludeSvc) > 0 {
			excludeList := strings.Split(*excludeSvc, "|")

			var excluded bool
			for _,ex := range excludeList {
				if val.ServiceName == ex {
					excluded = true
					break
				}
			}

			if excluded {
				continue
			}
		}

		svcTag := val.ServiceName + ":" + val.Tag

		if v, ok := repliMap[svcTag]; ok {
			if v.Status != "finished" {
				svcTag = svcTag + ":" + v.Status
			}
		}

		allSvcTags = append(allSvcTags, svcTag)
	}

	return strings.Join(allSvcTags, ",")
}

func getReplication(env string) {

	client := &http.Client{}

	var policyId string
	if env == "alpha" {
		policyId = "1"
	} else if env == "beta" {
		policyId = "3"
	}

	request, err := http.NewRequest("GET", "https://[harbor-host]/api/jobs/replication?policy_id="+ policyId +"&page=1&page_size=200", nil)
	if err != nil {
		log.Println(err)
	}

	auth := "admin:password"
	basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	request.Header.Add("Authorization", basicAuth)

	resp, err := client.Do(request)
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
	}

	//fmt.Println(string(content))

	var replicationStatus []ReplicationStatus
	if err := json.Unmarshal(content, &replicationStatus); err != nil {
		fmt.Println("Invalid json format !")
		return
	}

	for _, v := range replicationStatus {
		idx := strings.Index(v.Repository, "/")
		svcName := v.Repository[idx+1:] + ":" + v.Tags[len(v.Tags)-1]

		if rs, ok := repliMap[svcName]; ok {
			if rs.Id >= v.Id {
				continue
			}
		}
		repliMap[svcName] = v
	}
}
