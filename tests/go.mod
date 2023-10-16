module gorm.io/gorm/tests

go 1.16

require (
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/google/uuid v1.3.1
	github.com/jackc/pgx/v5 v5.4.3 // indirect
	github.com/jinzhu/now v1.1.5
	github.com/lib/pq v1.10.9
	golang.org/x/crypto v0.13.0 // indirect
	gorm.io/driver/mysql v1.5.1
	gorm.io/driver/postgres v1.5.2
	gorm.io/driver/sqlite v1.5.3
	gorm.io/driver/sqlserver v1.5.2
	gorm.io/gorm v1.25.2-0.20230610234218-206613868439
)

replace gorm.io/gorm => ../
