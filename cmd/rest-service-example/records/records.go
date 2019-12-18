package records

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
)

type Record struct {
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Phone string `json:"phone"`
}

func SelectAll(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/html")
	w.WriteHeader(200)
	_, err := w.Write([]byte("<h1>records.SelectAll: under construction</h1>\n"))
	if err != nil {
		log.Errorf("PostHandler, w.Write: %v\n", err)
	}
}

func Select(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.ParseUint(vars["id"], 10, 64)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v\n", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	row := conn.QueryRow(context.Background(),
		"SELECT id, name, phone FROM phonebook WHERE id = $1",
		id)

	var rec Record
	err = row.Scan(&rec.Id, &rec.Name, &rec.Phone)
	if err == pgx.ErrNoRows {
		w.WriteHeader(404)
		return
	}

	if err != nil {
		log.Errorf("Unable to SELECT: %v\n", err)
		w.WriteHeader(500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err = json.NewEncoder(w).Encode(rec)
	if err != nil {
		log.Errorf("Unable to encode json: %v\n", err)
		w.WriteHeader(500)
		return
	}
}

func Insert(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	var rec Record
	err := json.NewDecoder(r.Body).Decode(&rec)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v\n", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	row := conn.QueryRow(context.Background(),
		"INSERT INTO phonebook (name, phone) VALUES ($1, $2) RETURNING id",
		rec.Name, rec.Phone)
	var id uint64
	err = row.Scan(&id)
	if err != nil {
		log.Errorf("Unable to INSERT: %v\n", err)
		w.WriteHeader(500)
		return
	}

	resp := make(map[string]string, 10)
	resp["id"] = strconv.FormatUint(id, 10)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		log.Errorf("Unable to encode json: %v\n", err)
		w.WriteHeader(500)
		return
	}
}

func Update(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.ParseUint(vars["id"], 10, 64)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	var rec Record
	err = json.NewDecoder(r.Body).Decode(&rec)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v\n", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	ct, err := conn.Exec(context.Background(),
		"UPDATE phonebook SET name = $2, phone = $3 WHERE id = $1",
		id, rec.Name, rec.Phone)
	if err != nil {
		log.Errorf("Unable to UPDATE: %v\n", err)
		w.WriteHeader(500)
		return
	}

	if ct.RowsAffected() == 0 {
		w.WriteHeader(404)
		return
	}

	w.WriteHeader(200)
}

func Delete(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.ParseUint(vars["id"], 10, 64)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v\n", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	ct, err := conn.Exec(context.Background(), "DELETE FROM phonebook WHERE id = $1", id)
	if err != nil {
		log.Errorf("Unable to DELETE: %v\n", err)
		w.WriteHeader(500)
		return
	}

	if ct.RowsAffected() == 0 {
		w.WriteHeader(404)
		return
	}

	w.WriteHeader(200)
}
