package sqldb

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// NewDB creates a new database connection using the given MySQL connection string.
// The function returns a sqlx.DB pointer or an error if the connection failed.
func NewDB(path string) (*sqlx.DB, error) {
	path = fmt.Sprintf("%s?parseTime=true&loc=Local", path)

	client, err := sqlx.Open("mysql", path)
	if err != nil {
		return nil, err
	}

	if err = client.Ping(); err != nil {
		return nil, err
	}

	client.SetMaxOpenConns(140)
	client.SetMaxIdleConns(20)
	client.SetConnMaxLifetime(time.Minute * 5)

	return client, nil
}
