package main

import (
	"context"
	"github.com/afiskon/go-rest-service-example/cmd/rest-service-example/records"
	"github.com/afiskon/go-rest-service-example/cmd/rest-service-example/migrate"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strings"
)

func initViper(configPath string) {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("restexample")

	viper.SetDefault("loglevel", "debug")
	viper.SetDefault("listen", "localhost:8080")
	viper.SetDefault("db.url", "postgres://restservice@localhost/restservice?sslmode=disable&pool_max_conns=10")

	if configPath != "" {
		log.Infof("Parsing config: %s", configPath)
		viper.SetConfigFile(configPath)
		err := viper.ReadInConfig()
		if err != nil {
			log.Fatalf("Unable to read config file: %s", err)
		}
	} else {
		log.Infof("Config file is not specified.")
	}
}

func migrateDatabase(conn *pgx.Conn) {
	migrator, err := migrate.NewMigrator(conn, "schema_version")
	if err != nil {
		log.Fatalf("Unable to create a migrator: %v", err)
	}

	err = migrator.LoadMigrations("./migrations")
	if err != nil {
		log.Fatalf("Unable to load migrations: %v", err)
	}

	err = migrator.Migrate(func(err error) (retry bool) {
		log.Infof("Commit failed during migration, retrying. Error: %v", err)
		return true
	})

	if err != nil {
		log.Fatalf("Unable to migrate: %v", err)
	}

	ver, err := migrator.GetCurrentVersion()
	if err != nil {
		log.Fatalf("Unable to get current schema version: %v", err)
	}

	log.Infof("Migration done. Current schema version: %v", ver)
}

func initHandlers(pool *pgxpool.Pool) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/records",
		func(w http.ResponseWriter, r *http.Request) {
			records.SelectAll(pool, w, r)
		}).Methods("GET")

	r.HandleFunc("/api/v1/records/{id:[0-9]+}",
		func(w http.ResponseWriter, r *http.Request) {
			records.Select(pool, w, r)
		}).Methods("GET")

	r.HandleFunc("/api/v1/records",
		func(w http.ResponseWriter, r *http.Request) {
			records.Insert(pool, w, r)
		}).Methods("POST")

	r.HandleFunc("/api/v1/records/{id:[0-9]+}",
		func(w http.ResponseWriter, r *http.Request) {
			records.Update(pool, w, r)
		}).Methods("PUT")

	r.HandleFunc("/api/v1/records/{id:[0-9]+}",
		func(w http.ResponseWriter, r *http.Request) {
			records.Delete(pool, w, r)
		}).Methods("DELETE")
	return r
}

func run(configPath string, skipMigration bool) {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	initViper(configPath)

	logLevelString := viper.GetString("loglevel")
	logLevel, err := log.ParseLevel(logLevelString)
	if err != nil {
		log.Fatalf("Unable to parse loglevel: %s", logLevelString)
	}

	log.SetLevel(logLevel)

	dbURL := viper.GetString("db.url")
	log.Infof("Using DB URL: %s", dbURL)

	pool, err := pgxpool.Connect(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connection to database: %v", err)
	}
	defer pool.Close()
	log.Infof("Connected!")

	if !skipMigration {
		conn, err := pool.Acquire(context.Background())
		if err != nil {
			log.Fatalf("Unable to acquire a database connection: %v", err)
		}
		migrateDatabase(conn.Conn())
		conn.Release()
	}

	listenAddr := viper.GetString("listen")
	log.Infof("Starting HTTP server at %s...", listenAddr)
	http.Handle("/", initHandlers(pool))
	err = http.ListenAndServe(listenAddr, nil)
	if err != nil {
		log.Fatalf("http.ListenAndServe: %v", err)
	}

	log.Info("HTTP server terminated")
}

func main() {
	var configPath string
	var skipMigration bool

	rootCmd := cobra.Command{
		Use:     "rest-service-example",
		Version: "v1.0",
		Run: func(cmd *cobra.Command, args []string) {
			run(configPath, skipMigration)
		},
	}

	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Config file path")
	rootCmd.Flags().BoolVarP(&skipMigration, "skip-migration", "s", false, "Skip migration")
	err := rootCmd.Execute()
	if err != nil {
		// Required arguments are missing, etc
		return
	}
}
