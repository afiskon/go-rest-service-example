package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"text/template"
	"time"
)

// http.Client wrapper for adding new methods, particularly sendJsonReq
type httpClient struct {
	parent http.Client
}

// A bit more convenient method for sending requests to the HTTP server
func (client *httpClient) sendJsonReq(method, url string, reqBody []byte) (resp *http.Response, resBody []byte, err error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err = client.parent.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	resBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return resp, resBody, nil
}

func StartPostgreSQL() (confPath string, cleaner func()) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Panicf("[StartPostgreSQL] dockertest.NewPool failed: %v", err)
	}

	resource, err := pool.Run(
		"postgres", "11",
		[]string{
			"POSTGRES_DB=restservice",
			"POSTGRES_PASSWORD=s3cr3t",
		},
	)
	if err != nil {
		log.Panicf("[StartPostgreSQL] pool.Run failed: %v", err)
	}

	// PostgreSQL needs some time to start.
	// Port forwarding always works, thus net.Dial can't be used here.
	connString := "postgres://postgres:s3cr3t@"+resource.GetHostPort("5432/tcp")+"/restservice?sslmode=disable"
	attempt := 0
	ok := false
	for attempt < 20 {
		attempt++
		conn, err := pgx.Connect(context.Background(), connString)
		if err != nil {
			log.Infof("[StartPostgreSQL] pgx.Connect failed: %v, waiting... (attempt %d)", err, attempt)
			time.Sleep(1 * time.Second)
			continue
		}

		_ = conn.Close(context.Background())
		ok = true
		break
	}

	if !ok {
		_ = pool.Purge(resource)
		log.Panicf("[StartPostgreSQL] couldn't connect to PostgreSQL")
	}

	tmpl, err := template.New("config").Parse(`
loglevel: debug
listen: 0.0.0.0:8080
db:
  url: {{.ConnString}}
`)
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartPostgreSQL] template.Parse failed: %v", err)
	}

	configArgs := struct {
		ConnString string
	} {
		ConnString: connString,
	}
	var configBuff bytes.Buffer
	err = tmpl.Execute(&configBuff, configArgs)
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartPostgreSQL] tmpl.Execute failed: %v", err)
	}

	confFile, err := ioutil.TempFile("", "config.*.yaml")
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartPostgreSQL] ioutil.TempFile failed: %v", err)
	}

	log.Infof("[StartPostgreSQL] confFile.Name = %s", confFile.Name())

	_, err = confFile.WriteString(configBuff.String())
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartPostgreSQL] confFile.WriteString failed: %v", err)
	}

	err = confFile.Close()
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartPostgreSQL] confFile.Close failed: %v", err)
	}

	cleanerFunc := func() {
		// purge the container
		err := pool.Purge(resource)
		if err != nil {
			log.Panicf("[StartPostgreSQL] pool.Purge failed: %v", err)
		}

		err = os.Remove(confFile.Name())
		if err != nil {
			log.Panicf("[StartPostgreSQL] os.Remove failed: %v", err)
		}
	}

	return confFile.Name(), cleanerFunc
}

func StartCockroachDB() (confPath string, cleaner func()) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Panicf("[StartCockroachDB] dockertest.NewPool failed: %v", err)
	}

	opts := &dockertest.RunOptions{
		Repository: "cockroachdb/cockroach",
		Tag: "v19.2.3",
		Cmd: []string{"start-single-node", "--insecure"},
	}
	resource, err := pool.RunWithOptions(opts)

	if err != nil {
		log.Panicf("[StartCockroachDB] pool.Run failed: %v", err)
	}

	// CockroachDB needs some time to start.
	// Port forwarding always works, thus net.Dial can't be used here.
	connString := "postgres://root@"+resource.GetHostPort("26257/tcp")+"/postgres?sslmode=disable"
	attempt := 0
	ok := false
	for attempt < 20 {
		attempt++
		conn, err := pgx.Connect(context.Background(), connString)
		if err != nil {
			log.Infof("[StartCockroachDB] pgx.Connect failed: %v, waiting... (attempt %d)", err, attempt)
			time.Sleep(1 * time.Second)
			continue
		}

		_ = conn.Close(context.Background())
		ok = true
		break
	}

	if !ok {
		_ = pool.Purge(resource)
		log.Panicf("[StartCockroachDB] couldn't connect to CockroachDB")
	}

	tmpl, err := template.New("config").Parse(`
loglevel: debug
listen: 0.0.0.0:8080
db:
  url: {{.ConnString}}
`)
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartCockroachDB] template.Parse failed: %v", err)
	}

	configArgs := struct {
		ConnString string
	} {
		ConnString: connString,
	}
	var configBuff bytes.Buffer
	err = tmpl.Execute(&configBuff, configArgs)
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartCockroachDB] tmpl.Execute failed: %v", err)
	}

	confFile, err := ioutil.TempFile("", "config.*.yaml")
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartCockroachDB] ioutil.TempFile failed: %v", err)
	}

	log.Infof("[StartCockroachDB] confFile.Name = %s", confFile.Name())

	_, err = confFile.WriteString(configBuff.String())
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartCockroachDB] confFile.WriteString failed: %v", err)
	}

	err = confFile.Close()
	if err != nil {
		_ = pool.Purge(resource)
		log.Panicf("[StartCockroachDB] confFile.Close failed: %v", err)
	}

	cleanerFunc := func() {
		// purge the container
		err := pool.Purge(resource)
		if err != nil {
			log.Panicf("[StartCockroachDB] pool.Purge failed: %v", err)
		}

		err = os.Remove(confFile.Name())
		if err != nil {
			log.Panicf("[StartCockroachDB] os.Remove failed: %v", err)
		}
	}

	return confFile.Name(), cleanerFunc
}

// TestMain does the before and after setup
func TestMain(m *testing.M) {
	useCockroachEnv := os.Getenv("USE_COCKROACH_DB")
	var confPath string
	var stopDB func()
	if len(useCockroachEnv) > 0 {
		log.Infoln("[TestMain] About to start CockroachDB...")
		confPath, stopDB = StartCockroachDB()
		log.Infoln("[TestMain] CockroachDB started!")
	} else {
		log.Infoln("[TestMain] About to start PostgreSQL...")
		confPath, stopDB = StartPostgreSQL()
		log.Infoln("[TestMain] PostgreSQL started!")
	}

	// We should change directory, otherwise the service will not find `migrations` directory
	err := os.Chdir("../..")
	if err != nil {
		stopDB()
		log.Panicf("[TestMain] os.Chdir failed: %v", err)
	}

	cmd := exec.Command("./bin/rest-service-example", "-c", confPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		stopDB()
		log.Panicf("[TestMain] cmd.Start failed: %v", err)
	}
	log.Infof("[TestMain] cmd.Process.Pid = %d", cmd.Process.Pid)

	// We have to make sure the migration is finished and REST API is available before running any tests.
	// Otherwise there might be a race condition - the test see that API is unavailable and terminates,
	// pruning Docker container in the process which was running a migration.
	attempt := 0
	ok := false
	client := httpClient{}
	for attempt < 20 {
		attempt++
		_, _, err := client.sendJsonReq("GET", "http://localhost:8080/api/v1/records/0", []byte{})
		if err != nil {
			log.Infof("[TestMain] client.sendJsonReq failed: %v, waiting... (attempt %d)", err, attempt)
			time.Sleep(1 * time.Second)
			continue
		}

		ok = true
		break
	}

	if !ok {
		stopDB()
		_ = cmd.Process.Kill()
		log.Panicf("[TestMain] REST API is unavailable")
	}

	log.Infoln("[TestMain] REST API ready! Executing m.Run()")
	// Run all tests
	code := m.Run()

	log.Infoln("[TestMain] Cleaning up...")
	_ = cmd.Process.Signal(syscall.SIGTERM)
	stopDB()
	os.Exit(code)
}

func TestCRUD(t *testing.T) {
	t.Parallel()

	type PhonebookRecord struct {
		Id int64 `json:"id"`
		Name string `json:"name"`
		Phone string `json:"phone"`
	}
	client := httpClient{}

	// CREATE
	record := PhonebookRecord{
		Name:  "Alice",
		Phone: "123",
	}
	httpBody, err := json.Marshal(record)
	require.NoError(t, err)
	resp, respBody, err := client.sendJsonReq("POST", "http://localhost:8080/api/v1/records", httpBody)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	respBodyMap := make(map[string]string, 1)
	err = json.Unmarshal(respBody, &respBodyMap)
	require.NoError(t, err)
	recId, err := strconv.ParseInt(respBodyMap["id"], 10, 63)
	require.NoError(t, err)
	require.NotEqual(t, 0, recId)

	// READ
	resp, respBody, err = client.sendJsonReq("GET", "http://localhost:8080/api/v1/records/"+respBodyMap["id"], []byte{})
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	err = json.Unmarshal(respBody, &record)
	require.NoError(t, err)
	require.Equal(t, recId, record.Id)
	require.Equal(t, "Alice", record.Name)
	require.Equal(t, "123", record.Phone)

	// UPDATE
	record.Name = "Bob"
	record.Phone = "456"
	httpBody, err = json.Marshal(record)
	require.NoError(t, err)
	resp, respBody, err = client.sendJsonReq("PUT", "http://localhost:8080/api/v1/records/"+respBodyMap["id"], httpBody)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp, respBody, err = client.sendJsonReq("GET", "http://localhost:8080/api/v1/records/"+respBodyMap["id"], []byte{})
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	err = json.Unmarshal(respBody, &record)
	require.NoError(t, err)
	require.Equal(t, recId, record.Id)
	require.Equal(t, "Bob", record.Name)
	require.Equal(t, "456", record.Phone)

	// DELETE
	resp, respBody, err = client.sendJsonReq("DELETE", "http://localhost:8080/api/v1/records/"+respBodyMap["id"], httpBody)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp, respBody, err = client.sendJsonReq("GET", "http://localhost:8080/api/v1/records/"+respBodyMap["id"], []byte{})
	require.NoError(t, err)
	require.Equal(t, 404, resp.StatusCode)
}
